import logging
import os

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from db.db_utils import get_db_engine, execute_query

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GitHubAnalyzer:
    """
    Class for analyzing GitHub commit data from the database
    """

    def __init__(self, db_path="github_commits.db"):
        """
        Initialize with database path

        Args:
            db_path (str): Path to the SQLite database
        """
        self.engine = get_db_engine(db_path)
        self.output_dir = "output"

        os.makedirs(self.output_dir, exist_ok=True)

    def analyze_top_authors(self):
        """
        Analyze the top 5 authors by commit count

        Returns:
            pandas.DataFrame: Top 5 authors with their commit counts
        """
        logger.info("Analyzing top 5 authors by commit count")

        query = """
        SELECT 
            COALESCE(a.login, a.name, a.email, 'Unknown') as author,
            COUNT(cm.id) as commit_count
        FROM
            commits cm
        JOIN
            authors a ON cm.author_id = a.id
        GROUP BY
            a.id
        ORDER BY
            commit_count DESC
        LIMIT 5
        """

        results = execute_query(self.engine, query)

        df = pd.DataFrame(results)

        csv_path = os.path.join(self.output_dir, "top_authors.csv")
        df.to_csv(csv_path, index=False)
        logger.info(f"Top authors saved to {csv_path}")

        self._plot_top_authors(df)

        return df

    def analyze_top_committers(self):
        """
        Analyze the top 5 committers by commit count

        Returns:
            pandas.DataFrame: Top 5 committers with their commit counts
        """
        logger.info("Analyzing top 5 committers by commit count")

        query = """
        SELECT 
            COALESCE(c.login, c.name, c.email, 'Unknown') as committer,
            COUNT(cm.id) as commit_count
        FROM
            commits cm
        JOIN
            committers c ON cm.committer_id = c.id
        GROUP BY
            c.id
        ORDER BY
            commit_count DESC
        LIMIT 5
        """

        results = execute_query(self.engine, query)

        df = pd.DataFrame(results)

        csv_path = os.path.join(self.output_dir, "top_committers.csv")
        df.to_csv(csv_path, index=False)
        logger.info(f"Top committers saved to {csv_path}")

        self._plot_top_committers(df)

        return df

    def analyze_longest_author_streak(self):
        """
        Analyze the author with the longest commit streak by day

        Returns:
            dict: Author info with streak length
        """
        logger.info("Analyzing author with longest commit streak")

        query = """
        WITH daily_commits AS (
            SELECT
                a.id as author_id,
                COALESCE(a.login, a.name, a.email, 'Unknown') as author,
                DATE(cm.authored_at) as commit_date
            FROM
                commits cm
            JOIN
                authors a ON cm.author_id = a.id
            GROUP BY
                a.id, DATE(cm.authored_at)
        ),
        
        numbered_days AS (
            SELECT
                author_id,
                author,
                commit_date,
                ROW_NUMBER() OVER (PARTITION BY author_id ORDER BY commit_date) as row_num,
                julianday(commit_date) - ROW_NUMBER() OVER (PARTITION BY author_id ORDER BY commit_date) as group_id
            FROM
                daily_commits
        ),
        
        streaks AS (
            SELECT
                author_id,
                author,
                MIN(commit_date) as streak_start,
                MAX(commit_date) as streak_end,
                COUNT(*) as streak_length
            FROM
                numbered_days
            GROUP BY
                author_id, group_id
        )
        
        SELECT
            author,
            streak_start,
            streak_end,
            streak_length
        FROM
            streaks
        ORDER BY
            streak_length DESC
        LIMIT 1
        """

        results = execute_query(self.engine, query)

        if not results:
            logger.warning("No commit streaks found")
            return None

        result = results[0]

        df = pd.DataFrame([result])
        csv_path = os.path.join(self.output_dir, "longest_author_streak.csv")
        df.to_csv(csv_path, index=False)
        logger.info(f"Longest author streak saved to {csv_path}")

        logger.info(f"Longest streak: {result['author']} with {result['streak_length']} consecutive days")
        logger.info(f"Streak period: {result['streak_start']} to {result['streak_end']}")

        return result

    def analyze_commit_heatmap(self):
        """
        Generate a heatmap of commit counts by day of week and time of day

        Returns:
            pandas.DataFrame: DataFrame with commit counts by day and time
        """
        logger.info("Generating commit heatmap by day of week and time of day")

        query = """
        SELECT
            CAST(strftime('%w', authored_at) AS INTEGER) as day_of_week,
            CAST(strftime('%H', authored_at) AS INTEGER) as hour,
            COUNT(*) as commit_count
        FROM
            commits
        GROUP BY
            day_of_week, hour
        ORDER BY
            day_of_week, hour
        """

        results = execute_query(self.engine, query)

        df = pd.DataFrame(results)

        if df.empty:
            logger.warning("No commit data found for heatmap")
            return None

        all_days = list(range(7))
        all_hours = list(range(24))

        index = pd.MultiIndex.from_product([all_days, all_hours], names=['day_of_week', 'hour'])
        df_full = pd.DataFrame(index=index).reset_index()

        df_full = df_full.merge(df, on=['day_of_week', 'hour'], how='left')
        df_full['commit_count'] = df_full['commit_count'].fillna(0).astype(int)

        heatmap_data = df_full.pivot(index='day_of_week', columns='hour', values='commit_count')

        day_names = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
        heatmap_data.index = [day_names[i] for i in heatmap_data.index]

        day_order = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        heatmap_data = heatmap_data.reindex(day_order)

        hour_groups = [(0, 3), (3, 6), (6, 9), (9, 12), (12, 15), (15, 18), (18, 21), (21, 24)]
        grouped_data = pd.DataFrame(index=day_order)

        for start, end in hour_groups:
            col_name = f"{start:02d}-{end:02d}"
            grouped_data[col_name] = heatmap_data.iloc[:, start:end].sum(axis=1)

        csv_path = os.path.join(self.output_dir, "commit_heatmap.csv")
        grouped_data.to_csv(csv_path)
        logger.info(f"Commit heatmap data saved to {csv_path}")

        self._plot_commit_heatmap(grouped_data)

        return grouped_data

    def run_all_analyses(self):
        """
        Run all analyses
        """
        logger.info("Running all analyses")

        self.analyze_top_authors()
        self.analyze_top_committers()
        self.analyze_longest_author_streak()
        self.analyze_commit_heatmap()

        logger.info("All analyses completed")

    def _plot_top_authors(self, df):
        """
        Create a bar chart of top authors

        Args:
            df (pandas.DataFrame): DataFrame with author and commit_count columns
        """
        plt.figure(figsize=(10, 6))
        bars = plt.bar(df['author'], df['commit_count'])

        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width() / 2., height + 0.1,
                     f'{height:.0f}', ha='center', va='bottom')

        plt.title('Top 5 Authors by Commit Count')
        plt.xlabel('Author')
        plt.ylabel('Number of Commits')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()

        plot_path = os.path.join(self.output_dir, "top_authors.png")
        plt.savefig(plot_path)
        logger.info(f"Top authors plot saved to {plot_path}")
        plt.close()

    def _plot_top_committers(self, df):
        """
        Create a bar chart of top committers

        Args:
            df (pandas.DataFrame): DataFrame with committer and commit_count columns
        """
        plt.figure(figsize=(10, 6))
        bars = plt.bar(df['committer'], df['commit_count'])

        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width() / 2., height + 0.1,
                     f'{height:.0f}', ha='center', va='bottom')

        plt.title('Top 5 Committers by Commit Count')
        plt.xlabel('Committer')
        plt.ylabel('Number of Commits')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()

        plot_path = os.path.join(self.output_dir, "top_committers.png")
        plt.savefig(plot_path)
        logger.info(f"Top committers plot saved to {plot_path}")
        plt.close()

    def _plot_commit_heatmap(self, df):
        """
        Create a heatmap of commit counts by day and time

        Args:
            df (pandas.DataFrame): DataFrame with commit counts by day and time block
        """
        plt.figure(figsize=(12, 7))
        sns.heatmap(df, annot=True, cmap="YlGnBu", fmt='g')

        plt.title('Commit Frequency by Day of Week and Time of Day')
        plt.xlabel('Time of Day (hours)')
        plt.ylabel('Day of Week')
        plt.tight_layout()

        plot_path = os.path.join(self.output_dir, "commit_heatmap.png")
        plt.savefig(plot_path)
        logger.info(f"Commit heatmap plot saved to {plot_path}")
        plt.close()


if __name__ == "__main__":
    analyzer = GitHubAnalyzer()
    analyzer.run_all_analyses()
