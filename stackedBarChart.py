import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load the CSV file into a pandas DataFrame without header
df = pd.read_csv('genreDistr.csv', header=None, names=['Decade', 'Genre', 'Count'])

# List of genres to remove
genres_to_remove = ['Short', 'Sport', 'Adult', 'Reality-TV', 'Game-Show', 'Talk-Show', 'News']

# Remove rows with specified genres and Decade equal to '2030'
df_filtered = df[~((df['Genre'].isin(genres_to_remove)) | (df['Decade'] == 2030))]

# Group the filtered data by decade and genre, and sum the counts
df_grouped = df_filtered.groupby(['Decade', 'Genre'])['Count'].sum().reset_index()

# Get unique genres
num_unique_genres = len(df_grouped['Genre'].unique())

# Use the original color palette with additional colors (we have 22 genres and only 10 colors originally)
original_palette = sns.color_palette("bright")
additional_colors = sns.color_palette("deep", n_colors=num_unique_genres - len(original_palette))
custom_palette = original_palette + additional_colors
sns.set_palette(custom_palette)

# Pivot  data to create 100% stacked bar chart
df_pivot = df_grouped.pivot(index='Decade', columns='Genre', values='Count')
df_pivot = df_pivot.div(df_pivot.sum(axis=1), axis=0)

# Plot  data with reduced bar width
ax = df_pivot.plot(kind='bar', stacked=True, figsize=(10, 6), width=0.85)

# Plot styling
ax.legend(title='Genre', bbox_to_anchor=(1.01, 1), loc='upper left')
ax.set_xlabel('Decade')
ax.set_ylabel('Percentage')
ax.set_title('Distribution of Genres over the Decades')

# Save
plt.savefig('genreDistributionTighterCleaned.png', bbox_inches='tight')
