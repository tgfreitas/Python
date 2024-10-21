import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def plot_heatmap(df:pd.DataFrame, group:str, filter:str, savefig: bool=False) -> None:
    '''
    Creates a heatmap of topic correlations for a specified area using data from the 'score pulse' database.
    Optionally saves the plot to the code directory if 'savefig' is True.
  
    Paramaters:
    - df(pd.dataframe): DataFrame containing the data for correlation calculations.
    - group(str):The group to which the area belongs.group which the area belongs to.
    - filter: The area within the group for which to calculate correlations.
    Returns:
    - None

    '''
    df1 = df[df[group] == filter]
    pivoted = df1.pivot_table(values = 'indice',index = ['Lider'], columns = ['Topico']).reset_index()
    fig, ax = plt.subplots(figsize=(5, 5))
    sns.heatmap(pivoted.corr(), annot=True, fmt='.1f', linewidth=.5, ax = ax).set_title(f'{group} - {filter} ')
    plt.show()

    if savefig:
        fig.savefig(f"heatmap_{group}_{filter}.png",bbox_inches="tight")
    else:
        pass