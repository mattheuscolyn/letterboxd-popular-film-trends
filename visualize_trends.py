"""
Visualization script for Letterboxd Popular Film Trends dataset.
Creates various visualizations to analyze how films move in popularity rankings over time.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import numpy as np

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)

def load_and_prepare_data(csv_file='letterboxd_popular_history.csv'):
    """Load and prepare the dataset for visualization."""
    df = pd.read_csv(csv_file)
    
    # Normalize date format (some dates might be in different formats)
    def parse_date(date_str):
        try:
            # Try ISO format first
            return pd.to_datetime(date_str, format='%Y-%m-%d')
        except:
            try:
                # Try M/D/YYYY format
                return pd.to_datetime(date_str, format='%m/%d/%Y')
            except:
                return pd.to_datetime(date_str)
    
    df['Date'] = df['Snapshot Date'].apply(parse_date)
    df = df.sort_values('Date')
    
    return df

def plot_ranking_trends(df, film_ids=None, top_n=10):
    """Plot ranking trends over time for specific films or top N most consistent films."""
    fig, ax = plt.subplots(figsize=(16, 10))
    
    if film_ids is None:
        # Find films that appear in most snapshots (most consistent)
        film_appearances = df.groupby('Film Title')['Date'].nunique().sort_values(ascending=False)
        top_films = film_appearances.head(top_n).index.tolist()
        print(f"\nPlotting top {top_n} most consistent films:")
        for i, film in enumerate(top_films, 1):
            print(f"{i}. {film}")
    else:
        top_films = df[df['Film ID'].isin(film_ids)]['Film Title'].unique().tolist()
    
    # Plot each film
    colors = plt.cm.tab20(np.linspace(0, 1, len(top_films)))
    for idx, film_title in enumerate(top_films):
        film_data = df[df['Film Title'] == film_title].sort_values('Date')
        if len(film_data) > 0:
            ax.plot(film_data['Date'], film_data['Order'], 
                   marker='o', label=film_title, color=colors[idx], linewidth=2, markersize=4)
    
    ax.invert_yaxis()  # Lower order (rank) = higher on chart
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Ranking Position', fontsize=12)
    ax.set_title('Film Ranking Trends Over Time (Lower = More Popular)', fontsize=14, fontweight='bold')
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('ranking_trends.png', dpi=300, bbox_inches='tight')
    print("\nSaved: ranking_trends.png")
    plt.close()

def plot_film_entries_exits(df):
    """Plot how many new films enter and exit the top 1000 each week."""
    # Get unique dates
    dates = sorted(df['Date'].unique())
    
    entries = []
    exits = []
    
    for i, date in enumerate(dates):
        current_films = set(df[df['Date'] == date]['Film ID'].unique())
        
        if i > 0:
            prev_films = set(df[df['Date'] == dates[i-1]]['Film ID'].unique())
            # New entries
            new_entries = len(current_films - prev_films)
            # Films that exited
            new_exits = len(prev_films - current_films)
            entries.append(new_entries)
            exits.append(new_exits)
        else:
            entries.append(0)
            exits.append(0)
    
    fig, ax = plt.subplots(figsize=(14, 6))
    x = dates
    width = (dates[1] - dates[0]).days if len(dates) > 1 else 1
    
    ax.bar(x, entries, width=width*0.8, label='New Entries', alpha=0.7, color='green')
    ax.bar(x, [-e for e in exits], width=width*0.8, label='Exits', alpha=0.7, color='red')
    
    ax.axhline(y=0, color='black', linestyle='-', linewidth=0.8)
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Number of Films', fontsize=12)
    ax.set_title('Films Entering and Exiting Top 1000 Each Week', fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    plt.savefig('entries_exits.png', dpi=300, bbox_inches='tight')
    print("Saved: entries_exits.png")
    plt.close()

def plot_genre_distribution(df):
    """Plot genre distribution over time."""
    # Expand genres (some films have multiple genres)
    genre_data = []
    for _, row in df.iterrows():
        if pd.notna(row['Genres']):
            genres = [g.strip() for g in str(row['Genres']).split(',')]
            for genre in genres:
                genre_data.append({
                    'Date': row['Date'],
                    'Genre': genre,
                    'Film ID': row['Film ID']
                })
    
    genre_df = pd.DataFrame(genre_data)
    
    # Get top 10 genres by total appearances
    top_genres = genre_df.groupby('Genre')['Film ID'].nunique().sort_values(ascending=False).head(10).index.tolist()
    
    # Calculate percentage of top 1000 for each genre over time
    dates = sorted(df['Date'].unique())
    genre_percentages = {genre: [] for genre in top_genres}
    
    for date in dates:
        date_films = set(df[df['Date'] == date]['Film ID'].unique())
        total_films = len(date_films)
        
        for genre in top_genres:
            genre_films = set(genre_df[(genre_df['Date'] == date) & (genre_df['Genre'] == genre)]['Film ID'].unique())
            percentage = (len(genre_films) / total_films * 100) if total_films > 0 else 0
            genre_percentages[genre].append(percentage)
    
    fig, ax = plt.subplots(figsize=(14, 8))
    for genre in top_genres:
        ax.plot(dates, genre_percentages[genre], marker='o', label=genre, linewidth=2, markersize=3)
    
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Percentage of Top 1000 Films', fontsize=12)
    ax.set_title('Genre Distribution in Top 1000 Films Over Time', fontsize=14, fontweight='bold')
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('genre_distribution.png', dpi=300, bbox_inches='tight')
    print("Saved: genre_distribution.png")
    plt.close()

def plot_rating_vs_ranking(df):
    """Plot relationship between rating and ranking position."""
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    # Scatter plot: Rating Value vs Ranking
    latest_date = df['Date'].max()
    latest_df = df[df['Date'] == latest_date].copy()
    
    axes[0].scatter(latest_df['Rating Value'], latest_df['Order'], alpha=0.5, s=50)
    axes[0].set_xlabel('Average Rating', fontsize=12)
    axes[0].set_ylabel('Ranking Position (Lower = More Popular)', fontsize=12)
    axes[0].set_title(f'Rating vs Ranking Position (as of {latest_date.date()})', fontsize=13, fontweight='bold')
    axes[0].invert_yaxis()
    axes[0].grid(True, alpha=0.3)
    
    # Box plot: Rating distribution by ranking quartile
    latest_df['Ranking_Quartile'] = pd.qcut(latest_df['Order'], q=4, labels=['Top 25%', '25-50%', '50-75%', 'Bottom 25%'])
    latest_df = latest_df.dropna(subset=['Rating Value', 'Ranking_Quartile'])
    
    sns.boxplot(data=latest_df, x='Ranking_Quartile', y='Rating Value', ax=axes[1])
    axes[1].set_xlabel('Ranking Quartile', fontsize=12)
    axes[1].set_ylabel('Average Rating', fontsize=12)
    axes[1].set_title('Rating Distribution by Ranking Quartile', fontsize=13, fontweight='bold')
    axes[1].grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig('rating_vs_ranking.png', dpi=300, bbox_inches='tight')
    print("Saved: rating_vs_ranking.png")
    plt.close()

def plot_most_consistent_films(df, top_n=20):
    """Create a heatmap showing the most consistent films and their rankings over time."""
    # Find films that appear in most snapshots
    film_appearances = df.groupby('Film Title')['Date'].nunique().sort_values(ascending=False)
    top_films = film_appearances.head(top_n).index.tolist()
    
    # Create matrix: films x dates
    dates = sorted(df['Date'].unique())
    heatmap_data = []
    
    for film_title in top_films:
        film_row = []
        for date in dates:
            film_data = df[(df['Film Title'] == film_title) & (df['Date'] == date)]
            if len(film_data) > 0:
                film_row.append(film_data['Order'].iloc[0])
            else:
                film_row.append(np.nan)  # Film not in top 1000 on this date
        heatmap_data.append(film_row)
    
    heatmap_df = pd.DataFrame(heatmap_data, index=top_films, columns=dates)
    
    fig, ax = plt.subplots(figsize=(16, 10))
    sns.heatmap(heatmap_df, cmap='RdYlGn_r', annot=False, fmt='.0f', 
                cbar_kws={'label': 'Ranking Position'}, ax=ax, linewidths=0.5, 
                xticklabels=False, yticklabels=True)
    ax.set_title(f'Top {top_n} Most Consistent Films - Ranking Heatmap', fontsize=14, fontweight='bold')
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Film', fontsize=12)
    
    # Set custom x-axis labels - show every Nth date to avoid overcrowding
    num_dates = len(dates)
    label_every = max(1, num_dates // 10)  # Show about 10-12 labels
    
    # Get tick positions (seaborn uses positions 0 to len(dates))
    x_ticks = list(range(0, num_dates, label_every))
    if num_dates - 1 not in x_ticks:  # Always include the last date
        x_ticks.append(num_dates - 1)
    
    x_tick_labels = [dates[i].strftime('%Y-%m-%d') if i in x_ticks else '' 
                     for i in range(num_dates)]
    
    # Set ticks and labels
    ax.set_xticks([i + 0.5 for i in x_ticks])  # Add 0.5 to center on heatmap cells
    ax.set_xticklabels([dates[i].strftime('%Y-%m-%d') for i in x_ticks], 
                       rotation=45, ha='right', fontsize=8)
    
    plt.tight_layout()
    plt.savefig('most_consistent_films_heatmap.png', dpi=300, bbox_inches='tight')
    print("Saved: most_consistent_films_heatmap.png")
    plt.close()

def generate_summary_stats(df):
    """Print summary statistics about the dataset."""
    print("\n" + "="*60)
    print("DATASET SUMMARY")
    print("="*60)
    print(f"Total snapshots: {df['Date'].nunique()}")
    print(f"Date range: {df['Date'].min().date()} to {df['Date'].max().date()}")
    print(f"Total unique films: {df['Film ID'].nunique()}")
    print(f"Total records: {len(df)}")
    
    # Most consistent films
    print(f"\nTop 10 Most Consistent Films (by appearances):")
    film_appearances = df.groupby('Film Title')['Date'].nunique().sort_values(ascending=False)
    for i, (film, count) in enumerate(film_appearances.head(10).items(), 1):
        print(f"  {i}. {film}: {count} appearances")
    
    # Films with best average ranking
    print(f"\nTop 10 Films by Best Average Ranking:")
    avg_rankings = df.groupby('Film Title')['Order'].mean().sort_values()
    for i, (film, avg_rank) in enumerate(avg_rankings.head(10).items(), 1):
        appearances = df[df['Film Title'] == film]['Date'].nunique()
        print(f"  {i}. {film}: Avg Rank {avg_rank:.1f} ({appearances} appearances)")
    
    # Latest snapshot stats
    latest_date = df['Date'].max()
    latest_df = df[df['Date'] == latest_date]
    print(f"\nLatest Snapshot ({latest_date.date()}):")
    print(f"  Films in top 1000: {latest_df['Film ID'].nunique()}")
    print(f"  Top 5 films: {', '.join(latest_df.nsmallest(5, 'Order')['Film Title'].tolist())}")

def main():
    """Main function to generate all visualizations."""
    print("Loading data...")
    df = load_and_prepare_data()
    
    print("Generating summary statistics...")
    generate_summary_stats(df)
    
    print("\n" + "="*60)
    print("GENERATING VISUALIZATIONS")
    print("="*60)
    
    print("\n1. Creating ranking trends plot...")
    plot_ranking_trends(df, top_n=10)
    
    print("\n2. Creating entries/exits plot...")
    plot_film_entries_exits(df)
    
    print("\n3. Creating genre distribution plot...")
    plot_genre_distribution(df)
    
    print("\n4. Creating rating vs ranking plot...")
    plot_rating_vs_ranking(df)
    
    print("\n5. Creating most consistent films heatmap...")
    plot_most_consistent_films(df, top_n=20)
    
    print("\n" + "="*60)
    print("All visualizations complete!")
    print("="*60)

if __name__ == "__main__":
    main()