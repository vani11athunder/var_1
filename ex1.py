import pandas as pd

def calculate_retention(reg_path, auth_path, max_day=30):
    reg_df = pd.read_csv(reg_path, names=['user_id', 'reg_date'])
    auth_df = pd.read_csv(auth_path, names=['user_id', 'auth_date'])

    reg_df['reg_date'] = pd.to_datetime(reg_df['reg_date'], unit='s').dt.date
    auth_df['auth_date'] = pd.to_datetime(auth_df['auth_date'], unit='s').dt.date
    
    merged = pd.merge(auth_df, reg_df, on='user_id', how='left')
    merged['day'] = (merged['auth_date'] - merged['reg_date']).dt.days
    merged = merged[(merged['day'] >= 0) & (merged['day'] <= max_day)]

    daily_active = merged.drop_duplicates(['reg_date', 'user_id', 'day'])
    
    cohort_size = reg_df.groupby('reg_date')['user_id'].nunique().rename('cohort_size')
    dates = cohort_size.index.tolist()
    days = list(range(0, max_day+1))
    multi_index = pd.MultiIndex.from_product([dates, days], names=['reg_date', 'day'])
    full_grid = pd.DataFrame(index=multi_index).reset_index()
    
    active_users = daily_active.groupby(['reg_date', 'day']).size().reset_index(name='active_users')

    retention_data = pd.merge(full_grid, active_users, on=['reg_date', 'day'], how='left')
    retention_data = pd.merge(retention_data, cohort_size, on='reg_date', how='left')
    
    retention_data['active_users'] = retention_data['active_users'].fillna(0)
    retention_data['retention'] = retention_data['active_users'] / retention_data['cohort_size'] * 100
  
    retention_pivot = retention_data.pivot_table(
        index='reg_date',
        columns='day',
        values='retention',
        fill_value=0.0
    )
    
    retention_pivot = retention_pivot.sort_index(axis=1)
    
    return retention_pivot

if __name__ == "__main__":
    retention_table = calculate_retention(
        'shared/problem1-reg_data.csv',
        'shared/problem1-auth_data.csv'
    )
    print(retention_table)
