import os.path
import click
import requests
from pathlib import Path
import pandas as pd
import geopandas as gpd
import json
import matplotlib.pyplot as plt
pd.set_option('display.max_rows', None)

# Location of projects CSV file and location where data is downloaded and processed
reportsPath = 'C:\\Users\\wisam\\Desktop\\Report\\'

# Fetch raw OSM state file
def osm_fetch(url):
    filename = Path(url).name
    filepath = reportsPath + filename
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        
        size = int(r.headers["Content-Length"].strip())
        pbar = click.progressbar(length=size, label=f"Downloading {filename}")

        with open(filepath, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                pbar.update(len(chunk))
                
        pbar.render_finish()
    return filepath

# Fetch project tasks file
def fetch_project_tasks(project_id):
    tasks_file = Path(reportsPath, f'{project_id}_tasks.geojson')
    with requests.get(f'http://tasks.opensidewalks.com/api/v2/projects/{project_id}/tasks/') as resp:
        with open(tasks_file,'w',) as f:
            json.dump(resp.json(), f)
    return str(tasks_file)

downloadedOSM = set()
projectsFile = f'{reportsPath}projectIDs.csv'
projectIDs = pd.read_csv(projectsFile)

# For each project in the input file
for index, row in projectIDs.iterrows():
    osm_url = row['osm_url']
    osm_file = reportsPath + Path(osm_url).name
    
    # Fetch relevant state file if it has not already been downloaded
    if not osm_url in downloadedOSM and not os.path.exists(osm_file):
        osm_file = osm_fetch(osm_url)
        print(f'\rDownloaded OSM file: {osm_file}\n')
        downloadedOSM.add(osm_url)
        # Extract road centerlines from OSM PBF file
        get_ipython().system('ogr2ogr -where "highway in (\'trunk\',\'primary\',\'secondary\',\'tertiary\',\'unclassified\',\'residential\')" -f GeoJSON {osm_file}.roads.geojson {osm_file} lines')
        print(f'Generated GeoJSON roads file: {osm_file}.roads.geojson\n')
    
    # Fetch project tasks file
    tasks_file = fetch_project_tasks(row['project_id'])
    print(f'Downloaded tasks file: {tasks_file}\n')
    tasks = gpd.read_file(tasks_file)
    print(f'Total number of tasks by {tasks.groupby("taskStatus")["taskId"].count()}\n')
    
    roads = gpd.read_file(Path(reportsPath, f'{osm_file}.roads.geojson'))
    # Filter OSM roads by project tasks region
    relevant_roads = gpd.sjoin(roads, tasks, how="inner", predicate="intersects", rsuffix="_proj")
    
    # Convert CRS to avoid warning messages
    relevant_roads.crs = "EPSG:4326"
    relevant_roads = relevant_roads.to_crs(crs=3857)
    
    print(f'Total length of roads: {relevant_roads.length.sum():.2f}\n')
    total_relevant_length = relevant_roads.length.sum()
    
    # Create a list of the different status within the current project
    task_status_list = tasks.groupby('taskStatus')['taskStatus'].first()
    plotValues = []
    
    # For each status, calculate the centerline length and store the % in plotValues
    for task_status in task_status_list:
        print(f'Processing: {task_status}\n')
        joined = relevant_roads[relevant_roads['taskStatus'] == task_status]
        print(f'\tFeatures count ({task_status}): {joined["taskId"].count()}\n')
        print(f'\tTotal length of roads ({task_status}): {joined.length.sum():.2f}\n')
        plotValues.append(joined.length.sum()/total_relevant_length*100.0)
    
    # Plot the project's different status in a pie chart after sorting them descendingly by percentage
    fig, ax = plt.subplots()
    ax.set_title(f'Project {row["project_id"]}')
    patches, texts = plt.pie(plotValues, startangle=90, radius=1.2)
    labels = ['{0} - {1:1.2f} %'.format(i,j) for i,j in zip(task_status_list, plotValues)]
    patches, labels, dummy = zip(*sorted(zip(patches, labels, plotValues), key=lambda x: x[2], reverse=True))
    plt.legend(patches, labels, loc='best', bbox_to_anchor=(-0.1, 1.), fontsize=8)
