from fractions import Fraction

def shingles(tokens):return {tuple(tokens[i:i+2]) for i in range(max(0,len(tokens)-1))}
def exact_edges(docs):
    out=[]
    for i,a in enumerate(docs):
        for b in docs[i+1:]:
            x,y=shingles(a['tokens']),shingles(b['tokens']);n=len(x&y);d=len(x|y)
            if (d==0) or 4*n>=d:out.append((a['id'],b['id']))
    return out
