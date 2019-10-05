__all__ = (
    'chunkify',
)


def chunkify(l, n):
    if n == 1:
        return ([i] for i in l)
    else:
        return (l[i:i + n] for i in range(0, len(l), n))
