"""
functions that assist with processing a web service request
"""
import re

__all__ = [ 'is_content_type', 'match_accept', 'acceptable', 'order_accepts' ]

def is_content_type(label):
    """
    return True if the given format label should be interpreted as a content type (i.e. using 
    MIME-type syntax).  This implementation returns if it contains a '/' character.
    """
    return '/' in label

def match_accept(ctype, acceptable):
    """
    return the most specific content type of the two inputs if the two match each other, taking in 
    account wildcards, or None if the two do not match.  The returned content type will end in "/*" 
    if both input values end in "/*".
    """
    if ctype == acceptable or (acceptable.endswith('/*') and ctype.startswith(acceptable[:-1])):
        return ctype
    if ctype.endswith('/*') and acceptable.startswith(ctype[:-1]):
        return acceptable
    return None

def acceptable(ctype, acceptable):
    """
    return the first match of a given content type value to a list of acceptable content types
    """
    if len(acceptable) == 0:
        return ctype
    if ctype in ['*', '*/*']:
        return acceptable[0]
    for ct in acceptable:
        m = match_accept(ctype, ct)
        if m:
            return m

    return None

def order_accepts(accepts):
    """
    order the given accept values according to their q-value.  
    :param accepts:  the list of accept values with their q-values attached.  This can be given either 
                     as a str or a list of str, each representing the value of the HTTP Accept request 
                     header value.
                     :type accepts: str or list of str
    :return:  a list of the mime types in order of q-value.  (The q-values will be dropped.)
    """
    if isinstance(accepts, str):
        accepts = [a.strip() for a in accepts.split(',') if a]
    else:
        acc = []
        for a in accepts:
            acc.extend([b.strip() for b in a.split(',') if b])
        accepts = acc

    for i in range(len(accepts)):
        q = 1.0
        m = re.search(r';q=(\d+(\.\d+)?)', accepts[i])
        if m:
            q = float(m.group(1))
        accepts[i] = (re.sub(r';.*$', '', accepts[i]), q)

    accepts.sort(key=lambda a: a[1], reverse=True)
    return [a[0] for a in accepts if a[1] > 0]

