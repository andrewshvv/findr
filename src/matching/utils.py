from collections import defaultdict

import numpy as np
import pandas as pd


def compute_percentile(dist, bin_edges, cumulative_density):
    index = np.searchsorted(bin_edges[1:], dist, side='right')
    return cumulative_density[index - 1]


def group_user_data_for_index(rows):
    # Convert list of dictionaries to pandas DataFrame
    df = pd.DataFrame(rows)

    # Check for null values and drop them
    df = df.dropna(subset=['user_id', 'prompt_id', 'post_id'])

    # Group by 'user_id', 'prompt_id' and 'is_gpt_accepted' and aggregate the other columns into sets
    grouped_df = df.groupby([
        'user_id',
        'prompt_id',
    ]).agg({'post_id': 'unique'})

    # Reset index
    grouped_df = grouped_df.reset_index()

    # Convert pandas series to lists
    user_ids = grouped_df['user_id'].astype(int).values.tolist()
    user_prompts_ids = grouped_df['prompt_id'].astype(int).values.tolist()
    user_post_ids = grouped_df['post_id'].apply(lambda x: [int(i) for i in x]).values.tolist()

    return zip(user_ids, user_prompts_ids, user_post_ids)


def group_user_data_for_gpt_check(rows):
    # Convert list of dictionaries to pandas DataFrame
    df = pd.DataFrame(rows)

    # Check for null values and drop them
    df = df.dropna(subset=[
        'user_id',
        'prompt_id',
        'original_user_request',
        'context_from_gpt4',
        'prompt_status',
        'post_id',
        'process_status',
        'index_distance'
    ])

    # Group by 'user_id', 'prompt_id' and 'is_gpt_accepted' and aggregate the other columns into sets
    grouped_df = df.groupby([
        'user_id',
        'prompt_id',
        'original_user_request',
        'context_from_gpt4',
        'prompt_status'
    ]).agg({
        'post_id': 'unique',
        'process_status': list,
        'index_distance': list,
    })

    # Reset index
    grouped_df = grouped_df.reset_index()

    # Convert pandas series to lists
    user_ids = grouped_df['user_id'].astype(int).values.tolist()
    prompt_ids = grouped_df['prompt_id'].astype(int).values.tolist()
    original_user_requests = grouped_df['original_user_request'].values.tolist()
    contexts_from_gpt4 = grouped_df['context_from_gpt4'].values.tolist()

    is_first_search = [v == "approved" for v in grouped_df['prompt_status'].values.tolist()]
    is_all_rejected = [all([v == "rejected" for v in values]) for values in grouped_df['process_status'].values.tolist()]

    user_post_ids = grouped_df['post_id'].apply(lambda x: [int(i) for i in x]).values.tolist()
    user_index_distances = grouped_df['index_distance'].apply(lambda x: [float(i) for i in x]).values.tolist()
    user_process_statuses = grouped_df['process_status'].values.tolist()

    return zip(
        user_ids,
        prompt_ids,
        contexts_from_gpt4,
        original_user_requests,
        is_all_rejected,
        is_first_search,

        user_post_ids,
        user_process_statuses,
        user_index_distances,
    )
