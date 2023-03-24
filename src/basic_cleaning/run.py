#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(project="nyc_airbnb", job_type="basic_cleaning")
    logger.info("Fetching artifact")
    artifact = run.use_artifact(args.input_artifact)
    local_path = artifact.file()

    logger.info("Reading dataframe")
    df=pd.read_csv(local_path)
    
    #Pre-processing
    logger.info("Starting pre-processing...")
    # Drop outliers
    min_price = 10
    max_price = 350
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()
    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])
    
    outfile = f"{args.output_artifact}"
    df.to_csv(outfile, index=False)
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="type of output",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="output description",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="minimal price of airbnb room",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="maximum price of airbnb room",
        required=True
    )


    args = parser.parse_args()

    go(args)
