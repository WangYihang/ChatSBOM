from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import typer

from chatsbom.core.logging import console

app = typer.Typer()


@app.callback(invoke_without_command=True)
def main(
    input_data: str = typer.Option(
        'openapi_drift_data.csv', help='Input analysis data CSV',
    ),
    output_dir: str = typer.Option(
        'figures/drift', help='Directory to save plots',
    ),
):
    """
    Generate drift evolution charts from the analysis data.
    Separated from the main analysis for styling flexibility.
    """
    try:
        df_all = pd.read_csv(input_data)
        df_all['date'] = pd.to_datetime(df_all['date'])
    except Exception as e:
        console.print(f"[bold red]Failed to read input data: {e}[/bold red]")
        raise typer.Exit(1)

    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    for (owner, repo), df in df_all.groupby(['owner', 'repo']):
        console.print(f"Plotting [bold cyan]{owner}/{repo}[/bold cyan]...")
        df = df.sort_values('date')

        if len(df) < 2:
            continue

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)

        # Plot 1: Endpoint Counts & Activity
        ax1.plot(
            df['date'], df['code_count'], marker='o',
            label='Code Endpoints', color='#1f77b4', linewidth=2,
        )
        ax1.plot(
            df['date'], df['spec_count'], marker='s',
            label='OpenAPI Endpoints', color='#d62728', linewidth=2,
        )

        # Add bar chart for commit activity on the same axis (optional secondary Y)
        ax1_twin = ax1.twinx()
        ax1_twin.bar(
            df['date'], df['code_commits'], alpha=0.1,
            color='gray', label='Commit Activity', width=5,
        )
        ax1_twin.set_ylabel('Commits in Interval', color='gray')

        ax1.set_ylabel('Count')
        ax1.set_title(
            f'Drift Evolution: {owner}/{repo}\n(Agreement between Code and Docs)', fontsize=14,
        )
        ax1.legend(loc='upper left')
        ax1.grid(True, linestyle='--', alpha=0.6)

        # Plot 2: Percentages (The "Drift")
        ax2.plot(
            df['date'], df['overlap_pct'], marker='v',
            label='Consistency (Overlap %)', color='#2ca02c', linewidth=2,
        )
        ax2.fill_between(
            df['date'], df['overlap_pct'],
            100, color='#2ca02c', alpha=0.1,
        )

        ax2.plot(
            df['date'], df['stale_pct'], marker='x',
            label='Staleness (Zombie Specs %)', color='#ff7f0e', linestyle='--',
        )

        ax2.set_ylabel('Percentage (%)')
        ax2.set_xlabel('Release Date')
        ax2.set_ylim(0, 110)
        ax2.legend(loc='lower left')
        ax2.grid(True, linestyle='--', alpha=0.6)

        plt.tight_layout()
        save_path = out_path / f"{owner}_{repo}.png"
        plt.savefig(save_path, dpi=150)
        plt.close()
        console.print(f"  [green]Saved → {save_path}[/green]")
