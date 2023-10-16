use flatgeobuf::*;
use geozero::ToWkt;
use anyhow::{Context, Result};
use clap::Parser;
use polars::prelude::*;
use cloud::AmazonS3ConfigKey as Key;
use awscreds::Credentials;

#[derive(Parser)]
struct Cli{
    #[clap(short, long, value_parser, use_value_delimiter=true,value_delimiter = ',')]
    bbox: Option<Vec<f32>>,
}

// async fn get_geometries() -> Result<Vec<f32>>{
//     let mut fgb = HttpFgbReader::open("https://allofthedata.s3.us-west-2.amazonaws.com/acs/tracts_2019.flatgeohuff/tracts.fgb")
//     .await?
//     .select_bbox(-74.053001,40.662671,-73.836365,40.810691)
//     .await?;

//     let mut features = Vec::new()

//     while let Some(feature) = fgb.next().await? {
//         let props = feature.properties()?;
//         features.push(feature.to_wkt()?);
//     }
//     Ok(features)
// }

fn get_metrics()->Result<DataFrame>{

    let mut args = ScanArgsParquet::default();
    let cred = Credentials::new(
        Some("AKIAXFG5T5M4YDAKKFV5"),
        Some("8rKKjR6RMkl2tDA8JmhjZQa3Cnv8oxjwnP5LQn"),
        None,
        None,
        None
    )?;

    let cloud_options = cloud::CloudOptions::default().with_aws([
        (Key::AccessKeyId, &cred.access_key.unwrap()),
        (Key::SecretAccessKey, &cred.secret_key.unwrap()),
        (Key::Region, &"us-west-2".into()),
    ]);

    args.cloud_options =Some(cloud_options);
    println!("generated clound options");

    let df = LazyFrame::scan_parquet("s3://allofthedata/acs/tracts_2019.parquet",args)?
        .with_streaming(true)
        .select([
            col("GEOID")
        ])
        .collect()?;
    println!("DF");
    println!("{:?}", df);
    Ok(df)
}

#[tokio::main]
async fn main() ->Result<()>{
    let args = Cli::parse();
    println!("Trying to get metrics");
    get_metrics()?;
    Ok(())
}
