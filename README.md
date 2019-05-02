# Azure Cloud Storage to Local Environment

A simple node app to download all scripts from a given azure data lake or azure blob account to local environment.

Saves file with the original file modification date (on the cloud) and only downloads the file again if the file size changes or the file modification time changes.

It also downloads everything in parallel, which is great for many small files (<8kb), but does not affect big files (>16MB). I usually get capped around 20 Mbps download speed from azure data lake eitherway.

## Usage

This tool allows you to download a file structure from the server to a local folder (usually named "data") from either `Azure Data Lake` or `Azure Blob Storage`.

Before using it, you must install the azure dependencies, executing the npm install from the project root:

```
npm install
```

### Download From Azure Data Lake

To download from azure data lake, open `index-dls.js` and edit the first lines, putting the account name, tentant id and others details about your account and file origin/target.

After that, run the script in your terminal in the project root:

```
node index-dls.js
```

This should prompt you to login interactively to microsoft azure, and afterwards it will list all the files being downloaded.

Note that if a file from data lake has the same file size and modification date from its cloud version, it will not be downloaded.

Finally, the script will try to download every file in parallel, if the process did not finish it probably means it is still downloading, wait for it to finish to avoid downloading your files in half.

### Download from Azure Blob Storage

To download from blob storage, open `index-blob.js` and add your account information and account api key that you can get from the azure portal.

Execute the script:

```
node index-blob.js
```

This will download every blob file in parallel (if it is not the same locally) and the script will finish when every file is downloaded.

## Motivation

Backup, ease-of-development and other things require files to be downloaded locally.

Other tools that download files from azure duplicate them (i.e. re-download something with the same modification date and file size), which is pretty stupid.

## License

The license is MIT, you can do anything with this project and its resource as long as it does not affect me, some of the projects dependencies are made by third-party and may have different licenses.