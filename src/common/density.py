from chromadb.api.models import Collection
import numpy as np



def calculate_percentile(collection: Collection, embedding):
    # Get the embeddings for the support documents from the collection
    support_embeddings = collection.get(include=['embeddings'])['embeddings']

    dists = collection.query(query_embeddings=support_embeddings, n_results=2, include=['distances'])

    # Flatten the distances list, excluding the first element (which is an element's distance to itself)
    flat_dists = [item for sublist in dists['distances'] for item in sublist[1:]]

    # Compute a density function over the distances
    hist, bin_edges = np.histogram(flat_dists, bins=100, density=True)
    cumulative_density = np.cumsum(hist) / np.sum(hist)

    results = collection.query(query_embeddings=[embedding], n_results=10, include=['distances'])

    support_percentiles = []
    percentile = compute_percentile(results['distances'][0], bin_edges, cumulative_density)
    support_percentiles.append(percentile)

    for i, q in enumerate(dataset['question'][:20]):
        support = dataset['support'][i]
        top_result = results['documents'][i][0]

        if support != top_result:
            print(f"Question: {q} \nSupport: {support} \nTop result: {top_result}\n")
