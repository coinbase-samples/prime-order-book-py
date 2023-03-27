# Purpose

This is a single page real-time order book application built for Coinbase Prime using Python, SQL, and Dash, an open source framework for building Python data apps.  

All scripts are written in Python and tested with version 3.8.9.

## Installation

Simply clone the repo from your terminal window with the below command.

```bash
git clone URL-GOES-HERE
```

To install dependencies, run the following: 
```
pip install -r requirements.txt
```
You will also need API key credentials from a valid Coinbase Prime portfolio in order to use this application.

Add your credentials to ``example.env``, then run this command to rename that file:
```
cp example.env .env
```

You can now run the program with the below command, which will open the application in your default browser window: 

```
python orderbook.py
python frontend.py
```

For more information around Dash, please visit their [Github](https://github.com/plotly/dash) and [documentation](https://dash.plotly.com/introduction). 