const accountName = "blobmycompany";
const accountKey = "dJM4ukFDipJvewt2CJxlsGJbSvjgYJTTrXC/qz42FO8XxxxB8QhYzLfYokXFxxxDxA/xxx25xPOFZIyyyyYQxQ==";
const containerName = "randomcontainer";
const localDirectory = "./data";

const azure = require('azure-storage');

const {blobContainerExist, downloadBlobFile, downloadBlobContainerFiles} = require("./primitive.js");

(async function() {
	const blobService = azure.createBlobService(accountName, accountKey).withFilter(new azure.ExponentialRetryPolicyFilter());
	//console.log(blobService);
	const result = await blobContainerExist(blobService, containerName);
	if (!result || result instanceof Error) {
		return console.error("It seems that the specified root directory does not exist or is not a folder");
	}
	await downloadBlobContainerFiles(blobService, containerName, localDirectory);
})().catch(console.error);