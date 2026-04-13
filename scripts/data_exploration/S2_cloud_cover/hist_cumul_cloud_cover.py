import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

# File path
file_path = r"C:\Users\mlc\OneDrive - Universidade de Lisboa\Documents\investigacao-projectos-reviews-alunos-juris\projetos\DGT-S2CHANGE_2023\INCD_2024_07034_CPCA_A0\dados_cobert_nuvens_S2\Percentagem_de_nuvens.ods"

def create_clean_cumulative_histogram(path,tile):
    try:
        # Load and clean data
        df = pd.read_excel(path, engine='odf')
        # filter for the specific tile
        df = df[df['tile'] == tile]

        # Clean 'cloud_cover' column: remove '%' and convert to numeric
        df['cloud_cover'] = df['cloud_cover'].astype(str).str.replace('%', '', regex=False)
        df['cloud_cover'] = pd.to_numeric(df['cloud_cover'], errors='coerce')
        df = df.dropna(subset=['cloud_cover'])

        # Scale to 0-100 if stored as 0.0-1.0
        if df['cloud_cover'].max() <= 1.0 and df['cloud_cover'].mean() < 1.0:
            df['cloud_cover'] = df['cloud_cover'] * 100

        plt.figure(figsize=(10, 6))
        
        # Plot the main histogram
        n, bins, patches = plt.hist(df['cloud_cover'], bins=101, range=(0, 100), 
                                    cumulative=True, color='#3182bd', alpha=0.6, 
                                    label='Cumulative Distribution')

        # Calculate counts for your specific thresholds
        thresholds = [60, 65,70, 75, 80]
        stats_labels = []
        
        # Create a red dotted line for the total population
        total_count = len(df)
        plt.axhline(y=total_count, color='red', linestyle=':', linewidth=1.5)
        
        # Build the legend entries
        legend_elements = [mpatches.Patch(color='red', label=f'Total Population: {total_count}', linestyle=':')]
        
        for t in thresholds:
            count = (df['cloud_cover'] <= t).sum()
            percent = (count / total_count) * 100
            # Create a invisible proxy for the legend to show the text
            label_text = f'Files ≤ {t}%: {count} ({percent:.1f}%)'
            legend_elements.append(mpatches.Patch(color='none', label=label_text))
            
            # Add subtle vertical markers on the plot for reference
            plt.axvline(x=t, color='gray', linestyle='--', alpha=0.2)

        # Formatting the plot
        plt.title(f'Sentinel-2 Cloud Cover Analysis - Tile {tile}', fontsize=14, fontweight='bold')
        plt.xlabel('Cloud Cover Threshold (%)', fontsize=12)
        plt.ylabel('Total Number of Files', fontsize=12)
        plt.grid(axis='y', linestyle='-', alpha=0.2)
        plt.xticks(range(0, 101, 10))
        plt.xlim(0, 100)

        # Place the legend in a clear area (usually lower right for cumulative plots)
        plt.legend(handles=legend_elements, loc='lower right', frameon=True, 
                   title="Dataset Statistics", title_fontsize='11', fontsize='10')

        plt.tight_layout()
        plt.show()
        
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    create_clean_cumulative_histogram(file_path, tile="T29SNC")