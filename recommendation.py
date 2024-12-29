import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer

def generate_recommendations(customer_id, top_n=5):
    # Load data from BigQuery
    from google.cloud import bigquery
    client = bigquery.Client()
    
    query = f"""
    SELECT customer_id, category, SUM(quantity) as total_quantity
    FROM `{PROJECT_DATASET}.cleaned_customer_data`
    GROUP BY customer_id, category
    """
    data = client.query(query).to_dataframe()
    
    # Create a pivot table
    pivot_table = data.pivot(index='customer_id', columns='category', values='total_quantity').fillna(0)
    
    # Compute cosine similarity
    similarity = cosine_similarity(pivot_table)
    similarity_df = pd.DataFrame(similarity, index=pivot_table.index, columns=pivot_table.index)
    
    # Get similar users
    similar_users = similarity_df[customer_id].sort_values(ascending=False).index[1:]
    
    # Recommend categories based on similar users
    recommendations = data[data['customer_id'].isin(similar_users)]
    top_recommendations = (
        recommendations.groupby('category')['total_quantity']
        .sum()
        .sort_values(ascending=False)
        .head(top_n)
        .index.tolist()
    )
    return top_recommendations

# Example Usage
customer_id = "C12345"
print(generate_recommendations(customer_id))
