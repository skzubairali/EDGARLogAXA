import pandas as pd

df = pd.read_csv("log20170201/log20170201.csv")

# todo: un-comment below for testing 1 sample
df = df[df["ip"] == "107.23.85.jfd"]

df['timestamp'] = pd.to_datetime(df['date'] + ' ' + df['time'])


def get_groups(s, thresh='1min'):
    out = [1]+[0]*(len(s)-1)
    ref = s.iloc[0]
    session_id = 1
    for i, val in enumerate(s[1:]):
        if (val-ref) > pd.Timedelta(thresh):
            out[i+1] = session_id
            out[i] = session_id - 1
            session_id += 1
        else:
            out[i+1] = session_id
        ref = val
    return pd.Series(out, index=s.index)


df['Session'] = df.groupby('ip')['timestamp'].apply(get_groups)

df_size = df.groupby(['ip', 'Session'])['size'].sum().reset_index()
df_size = df_size.sort_values(['size'], ascending=False).head(10)
print(df_size)

df_count = df.groupby(['ip', 'Session']).size().reset_index(name='counts')
df_count = df_count.sort_values(['counts'], ascending=False).head(10)
print(df_count)

# todo: use to export output
# df.to_csv("tmp.csv")