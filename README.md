# Financial Big Data Project (FBD-Project)

This repository hosts the code and analysis for the EPFL course Financial Big Data (FIN-525). The project focuses on the empirical verification of the square root law of market impact, using E-Mini S&P 500 futures transaction data.

## Project Structure

At the root of the project, the following files are included:

- `eda.ipynb`: An exploratory data analysis notebook which provides initial insights into the dataset and its characteristics.
- `trade_impact.ipynb`: The main Jupyter notebook that runs the analysis and generates the key plots illustrating the project's findings.
- `utils/`: A directory containing utility scripts that support data loading, preprocessing, and processing.

Within the `utils` directory:

- `loading.py`: Defines the main paths (`DATA_DIR`, etc.) and contains the logic for loading the dataset.
- `preprocessing.py`: Contains functions and logic to prepare the raw data for analysis.
- `processing.py`: Includes the main processing logic of the dataset and functions designed to run in a multiprocessed manner for efficiency.

## Getting Started

### Prerequisites

Ensure that you have the following prerequisites installed on your system:

- Python 3.x
- Jupyter Notebook or Jupyter Lab
- Necessary Python packages: `numpy`, `pandas`, `matplotlib`, `multiprocessing`, etc.

### Installation

Clone the repository to your local machine:

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

## Contributing

Contributions to this project are welcome. Please open an issue to discuss proposed changes or open a pull request with your updates.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- EPFL for providing the framework and guidance for this analysis.
- The prof. Damien Challet, and the main TA Federico Baldi Lanfranchi.
