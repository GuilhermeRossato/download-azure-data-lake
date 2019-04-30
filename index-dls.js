const accountName = "xxxx";
const serverDirectory = "/";
const localDirectory = "./data";
const subscriptionId = "56298751-eeee-4a4a-93bb-03b222af0f6c";
const tenantId = "ffcae422-ef55-4bbb-aaaa-f82fb78d4324";

const msRestAzure = require('ms-rest-azure');
const adlsManagement = require("azure-arm-datalake-store");
const azure = require('azure-storage');
const {getDLSCredentials, listDLSFiles, recursivelyDownloadDLSFiles, stat} = require("./primitive.js");
const fs = require('fs');

(async function() {
	const credentials = await getDLSCredentials(tenantId);
	if (!(credentials instanceof msRestAzure.DeviceTokenCredentials)) {
		return console.error("Invalid credentials");
	}
	const fsClient = new adlsManagement.DataLakeStoreFileSystemClient(credentials, {subscription: subscriptionId});
	const rootDir = (await listDLSFiles(fsClient, accountName, "/"))
		.filter(a => ((serverDirectory === "/") || (a.pathSuffix === serverDirectory.substr(1).split("/")[0])))[0];
	if (!rootDir || rootDir.type !== "DIRECTORY") {
		return console.error("It seems that the specified root directory does not exist or is not a folder");
	}
	await recursivelyDownloadDLSFiles(fsClient, accountName, serverDirectory, localDirectory);
})().catch(console.error);