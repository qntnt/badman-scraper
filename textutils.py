
import difflib

def similarity(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, a, b).ratio()

def clean_address(s: str) -> str:
    s = s.replace(',', '').replace('-', ' ')
    words = s.split()
    words = filter(lambda w: w != '', words)
    words = map(lambda w: 'AVE' if w == 'AV' else w, words)
    words = list(words)
    if len(words) > 0:
        last = len(words) - 1
        if len(words[last]) < 5:
            words = words[:last]
    return ' '.join(words)
