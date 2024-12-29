from flask import Flask, request, jsonify
import pickle
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

# Download VADER lexicon for sentiment analysis
nltk.download('vader_lexicon')

app = Flask(__name__)

# Sentiment analysis function
sia = SentimentIntensityAnalyzer()

def sentiment_to_rating(sentiment_score):
    """Map sentiment score to rating on a scale from 1 to 5"""
    if sentiment_score >= 0.05:
        return 5  # Positive sentiment
    elif sentiment_score >= 0.0:
        return 4  # Slightly positive
    elif sentiment_score >= -0.05:
        return 3  # Neutral sentiment
    elif sentiment_score >= -0.3:
        return 2  # Slightly negative
    else:
        return 1  # Negative sentiment

class RecommendationModel:
    def __init__(self, data):
        self.data = data
        self.user_perfume_ratings = None
        self.item_similarity_df = None

    def preprocess_data(self):
        """Preprocess the input data and compute sentiment-based ratings."""
        self.data['sentiment_score'] = self.data['review'].apply(lambda x: sia.polarity_scores(str(x))['compound'])
        self.data['rating'] = self.data['sentiment_score'].apply(sentiment_to_rating)

        # Generate synthetic user_id and perfume_id for this example
        self.data['user_id'] = self.data.index  # Using the index as a user ID
        self.data['perfume_id'] = self.data.index  # Using the index as a perfume ID

        # Create the user-item matrix (ratings of perfumes by users)
        self.user_perfume_ratings = self.data.pivot(index='user_id', columns='perfume_id', values='rating').fillna(0)

    def compute_item_similarity(self):
        """Compute item similarity (cosine similarity between perfumes)."""
        item_similarity = cosine_similarity(self.user_perfume_ratings.T)  # Calculate similarity between perfumes
        self.item_similarity_df = pd.DataFrame(item_similarity, 
                                                index=self.user_perfume_ratings.columns, 
                                                columns=self.user_perfume_ratings.columns)

    def recommend(self, user_id, top_n=5):
        """Generate perfume recommendations for a specific user."""
        user_ratings = self.user_perfume_ratings.loc[user_id]
        rated_perfumes = user_ratings[user_ratings > 0].index.tolist()

        predicted_scores = {}
        for perfume in self.user_perfume_ratings.columns:
            if perfume not in rated_perfumes:
                weighted_sum = sum(
                    self.item_similarity_df.loc[perfume, rated_perfume] * user_ratings[rated_perfume]
                    for rated_perfume in rated_perfumes
                )
                similarity_sum = sum(
                    self.item_similarity_df.loc[perfume, rated_perfume]
                    for rated_perfume in rated_perfumes
                )
                predicted_scores[perfume] = weighted_sum / similarity_sum if similarity_sum > 0 else 0

        # Return the top N recommended perfumes
        recommended_perfumes = sorted(predicted_scores, key=predicted_scores.get, reverse=True)[:top_n]
        return recommended_perfumes

    @staticmethod
    def load_model(filename='perfume_recommendation_model.pkl'):
        """Load the trained model from a pickle file."""
        with open(filename, 'rb') as f:
            model = pickle.load(f)
        return model


# Load the model at the start of the application
model = RecommendationModel.load_model("perfume_recommendation_model.pkl")

@app.route('/')
def home():
    return "Perfume Recommendation Service"

@app.route('/recommend', methods=['GET'])
def recommend():
    user_id = request.args.get('user_id', type=int)
    top_n = request.args.get('top_n', default=5, type=int)
    
    if user_id is None:
        return jsonify({'error': 'user_id is required'}), 400
    
    recommended_perfumes = model.recommend(user_id=user_id, top_n=top_n)
    return jsonify({'recommended_perfumes': recommended_perfumes})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)

