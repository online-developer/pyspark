from os.path import dirname


def updir(dir, level):
    r = dir
    for _ in range(level):
        r = dirname(r)
    return r



def sel_cols(df ,xcld_cols):
    return [c for c in df.columns if c not in xcld_cols]
