# Running the preprocessing notebook

`Data PreProcessing.ipynb` walks through the preprocessing on a 5% sample of
the raw dataset, using the same functions as `scripts/data_preprocessor.py`.
It does not overwrite `data/processed`.

**In Docker (recommended):** `START.bat` / `./start` start the `jupyter-dev`
container. Open <http://localhost:8888> and run the notebook there.

**Raw data:** the notebook reads `data/raw/DNN-EdgeIIoT-dataset.csv`. If the
file is missing, it downloads the dataset from Kaggle, which needs your API
token in the repository's `kaggle/` folder (mounted into the container):

```plaintext
./kaggle/kaggle.json
```

**On the host instead of Docker:** place the token where the Kaggle client
looks for it (`C:\Users\<you>\.kaggle\kaggle.json` on Windows,
`~/.kaggle/kaggle.json` elsewhere) and install `requirements/jupyter-dev.txt`.

Never commit `kaggle.json`; `.gitignore` already excludes it.
