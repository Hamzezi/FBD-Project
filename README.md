# Financial Big Data Project (FBD-Project)

This repository hosts the code and analysis for the EPFL course Financial Big Data (FIN-525). The project focuses on the empirical verification of the square root law of market impact, using E-Mini S&P 500 futures transaction data.

## Project Structure

```bash
.
├── README.md
├── requirements.txt
├── eda.ipynb
├── trade_impact.ipynb
├── data/trade/
└── utils/
    ├── __init__.py
    ├── loading.py
    ├── preprocessing.py
    └── processing.py
```

At the root of the project, the following files are included:

- `eda.ipynb`: An exploratory data analysis notebook which provides initial insights into the dataset and its characteristics.
- `trade_impact.ipynb`: The main Jupyter notebook that runs the analysis and generates the key plots illustrating the project's findings.
- `utils/`: A directory containing utility scripts that support data loading, preprocessing, and processing.
- `plots/`: A directory containing our main plots.

Within the `utils` directory:

- `loading.py`: Defines the main paths (`DATA_DIR_PATH`, etc.) and contains the logic for loading the dataset.
- `preprocessing.py`: Contains functions and logic to prepare the raw data for analysis.
- `processing.py`: Includes the main processing logic of the dataset and functions designed to run in a multiprocessed manner for efficiency.

## Getting Started

### Prerequisites

Ensure that you have the following prerequisites installed on your system:

- Python 3.x
- Jupyter Notebook or Jupyter Lab
- Necessary Python packages: `numpy`, `pandas`, `matplotlib`, `multiprocessing`, etc.

## Data Setup for Reproducibility

Due to the large size of the dataset used in this analysis, it is not included in this repository. To reproduce the results:

1. Create a `data/` directory in the root of the project:

    ```bash
    mkdir data
    ```

2. Within the `data/` directory, place your `trade/` data folder that contains the Canadian transaction data files.

3. If you need to modify the path where the data is loaded from, you can do so by editing the `DATA_DIR_PATH` variable in the `utils/loading.py` module.

4. Ensure the data folder structure matches the expected format as defined in the data loading logic of `utils/loading.py`.

By following these steps, you can set up the data in a manner consistent with the codebase, allowing for seamless reproduction of the analysis.

## Installation

After setting up your data as described above, clone the repository to your local machine:

```bash
git clone https://github.com/your-username/FBD-Project.git
cd FBD-Project
```

Install the required Python packages:

```bash
pip install -r requirements.txt
```

### Usage

To start exploring the dataset, open the `eda.ipynb` notebook:

```bash
jupyter notebook eda.ipynb
```

For running the main analysis, open the `trade_impact.ipynb` notebook:

```bash
jupyter notebook trade_impact.ipynb
```

To utilize the multiprocessed processing functions, ensure that your dataset is structured correctly as per `loading.py` expectations and call the processing functions within `processing.py`.


## Acknowledgments

- EPFL for providing the framework and guidance for this analysis.
- The prof. Damien Challet, and the main TA Federico Baldi Lanfranchi.
