"""Specified deterministic test mechanism, not a production credential provider."""
import hashlib,hmac,math

def laplace_for_test(secret,release_key,bucket,scale):
    digest=hmac.new(secret.encode(),(release_key+'\0'+bucket).encode(),hashlib.sha256).digest()
    u=((int.from_bytes(digest[:8],'big') >> 12)+0.5)/2**52-0.5
    return -scale*(1 if u>=0 else -1)*math.log1p(-2*abs(u))
