import re

class Extractor:
    def __init__(self):
        self.blocked_domains = [
            'sentry.wixpress.com',
            'sentry.io',
            'sentry-next.wixpress.com',
            'example.com',
            'domain.com',
            'wixpress.com',
            'email.com',
            'yourdomain.com'
        ]
        self.blocked_extensions = ('.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp')

    def extract_email(self, text):
        if not text: return None
        
        # More robust regex that avoids common false positives in path-like strings
        # Looks for:
        # [starts with alphanum+chars] @ [alphanum_chars] . [2+ letter extension]
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        
        matches = set(re.findall(email_pattern, text))
        
        valid_emails = []
        for email in matches:
            email = email.lower()
            
            # Check for image extensions at the end (sometimes regex catches filename@2x.png)
            if email.endswith(self.blocked_extensions):
                continue
                
            # Check for blocked domains
            domain = email.split('@')[-1]
            if any(blocked in domain for blocked in self.blocked_domains):
                continue
            
            valid_emails.append(email)
        
        # Return the best candidate (shortest is often best as it's less likely to be a concatenated string, 
        # but sometimes 'info@' is better than 'specific_person@'. For now, list order or simple heuristics)
        # Prioritize 'info', 'contact', 'hello'
        priority_prefixes = ['info', 'contact', 'hello', 'office', 'support','mail']
        
        valid_emails.sort(key=lambda x: (
            not any(p in x for p in priority_prefixes), # True (1) matches come last
            len(x) # Shorter is usually better fallback
        ))

        return valid_emails[0] if valid_emails else None

    def extract_phone(self, text):
        if not text: return None
        
        phone_pattern = r'(?:(?:\+|00)\d{1,3}[-.\s]?)?(?:\(?\d{2,5}\)?[-.\s]?)?\d{3}[-.\s]?\d{3,4}'
        
        # Simple extraction - find all that look like phone numbers
        matches = list(re.finditer(phone_pattern, text))
        
        valid_phones = []
        for match in matches:
            raw_phone = match.group(0).strip()
            # Basic validation: check digit count
            digit_count = sum(c.isdigit() for c in raw_phone)
            if 6 <= digit_count <= 15: # Standard phone length range
                valid_phones.append(raw_phone)
            
        return valid_phones[0] if valid_phones else None
