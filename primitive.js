var azure = require('azure-storage');
var fs = require('fs');
var msRestAzure = require('ms-rest-azure');
var adlsManagement = require("azure-arm-datalake-store");

async function blobContainerExist(blobService, containerName) {
	return (await getContainerInfo(blobService, containerName)).exists;
}

function downloadBlobFile(blobService, containerName, fileName, outputPath, retryOnConnectionReset=true) {
	return new Promise((resolve, reject) => {
		if (!blobService) {
			reject(new Error("Invalid BlobService"));
		}
		if (typeof containerName !== "string") {
			reject(new Error("Container name must be a string"));
		}
		if (containerName.toLowerCase() !== containerName) {
			reject(new Error("Container name must be fully lowercase"));
		}
		if (typeof fileName !== "string") {
			reject(new Error("Azure blob file name must be a string"));
		}
		if (typeof outputPath !== "string") {
			reject(new Error("Output path must be a string"));
		}
		var stream = fs.createWriteStream(outputPath);
		blobService.getBlobToStream(containerName, fileName, stream, function(error, result, response) {
			if (error) {
				if (retryOnConnectionReset && error.code === "ECONNRESET") {
					return downloadBlobFile(blobService, containerName, fileName, outputPath, false).then(resolve).catch(reject);
				}
				return reject(error);
			}
			if (!result) {
				return reject(new Error("Missing BlobResult from Operations"))
			}
			if (!response) {
				return reject(new Error("Missing Response object describing the request"));
			}
			if (!response.isSuccessful || response.statusCode !== 200) {
				return reject(new Error("Response returned unsucessfull with status code \""+response.statusCode+"\""));
			}
			return resolve(result);
		});
	});
}

function listContainerFiles(blobService, containerName) {
	return new Promise((resolve, reject) => {
		if (typeof containerName !== "string") {
			reject(new Error("Container name must be a string"));
		}
		blobService.listBlobsSegmented(containerName, null, {}, function (error, response) {
			if (error) {
				return reject(error);
			}
			if (!response) {
				return reject(new Error("Empty response"))
			}
			if (!response.entries) {
				return reject(new Error("Missing response entries"));
			}
			if (response.continuationToken) {
				return blobService.listBlobsSegmented(containerName, response.continuationToken, {}, function(error2, response2) {
					if (error) {
						return reject(error);
					}
					if (!response2 || !response2.entries) {
						reject(new Error("Empty response"));
					}
					var result = [];
					for (var i = 0; i <= response.entries.length; i++) {
						result.push(response.entries[i]);
					}
					for (var i = 0; i <= response2.entries.length; i++) {
						result.push(response2.entries[i]);
					}
					resolve(result);
				})
			} else {
				return resolve(response.entries);
			}
		});
	});
}

async function downloadBlobContainerFiles(blobService, containerName, destination) {
	var files = await listContainerFiles(blobService, containerName);
	for (var i=0; i < files.length; i++) {
		//console.log(files[i].contentSettings);
		var blobResult = files[i];
		var blobName = blobResult.name;
		var blobModified = blobResult.lastModified;
		var blobDestination = destination+"/"+blobName;
		var blobSize = blobResult.contentLength;

		if (blobResult.blobType !== "BlockBlob") {
			console.log("Skipping unknown blobType \""+blobResult.blobType+"\"");
			continue;
		}

		if (await file_exists(blobDestination)) {
			var localModifiedTime = new Date(await _getFileTime(blobDestination));
			var modificationTime = new Date(blobModified);
			if (1 >= Math.abs(localModifiedTime-modificationTime)) {
				console.log("Skipping up to date file \""+blobDestination+"\""); // is up to date
				continue;
			}
		}
		if (blobSize < 1024*1024*1024) {
			console.log("Downloading \""+blobDestination+"\": "+treatByteString(blobSize));
			var result = await downloadBlobFile(blobService, containerName, blobName, blobDestination);
			if (await file_exists(blobDestination)) {
				var setTimeResult = await _setFileTime(blobDestination, new Date(blobModified));
			}
		} else {
			console.log("Skipping large file \""+blobName+"\": "+treatByteString(blobSize));
		}
		//await downloadBlobFile(blobService, containerName, blobName, blobDestination);
	}
}

function getContainerInfo(blobService, containerName) {
	return new Promise((resolve, reject) => {
		if (!(blobService instanceof azure.BlobService)) {
			//reject(new Error("Invalid BlobService"));
		}
		if (typeof containerName !== "string") {
			reject(new Error("Container name must be a string"));
		}
		if (containerName.toLowerCase() !== containerName) {
			reject(new Error("Container name must be fully lowercase"));
		}
		blobService.doesContainerExist(containerName, function(err, result) {
			if (err) {
				return reject(err);
			}
			if (!result) {
				return reject(new Error("Result is empty"));
			}
			return resolve(result);
		});
	});
}



async function file_exists(path) {
	return !!(await stat(path));
}

function mkdir(path) {
	return new Promise((resolve, reject) => {
		fs.mkdir(path, { recursive: true }, (err) => {
			if (err) {
				reject(err);
			} else {
				resolve(true);
			}
		});
	});
}

function stat(path) {
	return new Promise((resolve, reject) => {
		fs.stat(path, function(err, stats) {
			if (err) {
				if (err.code === "ENOENT") {
					resolve();
				} else {
					reject(err);
				}
			} else {
				resolve({
					"access-time": stats["atime"],
					"modified-time": stats["mtime"],
					"size": stats["size"],
					"mode": stats["mode"],
					"permissions": {
						"others": {
							"execute": (stats["mode"] & 1 ? true : false),
							"write": (stats["mode"] & 2 ? true : false),
							"read": (stats["mode"] & 4 ? true : false)
						},
						"group": {
							"execute": (stats["mode"] & 10 ? true : false),
							"write": (stats["mode"] & 20 ? true : false),
							"read": (stats["mode"] & 40 ? true : false)
						},
						"user": {
							"execute": (stats["mode"] & 100 ? true : false),
							"write": (stats["mode"] & 200 ? true : false),
							"read": (stats["mode"] & 400 ? true : false)
						},
						"code": [
							(stats["mode"] & 1 ? "r" : "-"),
							(stats["mode"] & 2 ? "w" : "-"),
							(stats["mode"] & 4 ? "x" : "-"),
							(stats["mode"] & 10 ? "r" : "-"),
							(stats["mode"] & 20 ? "w" : "-"),
							(stats["mode"] & 40 ? "x" : "-"),
							(stats["mode"] & 100 ? "r" : "-"),
							(stats["mode"] & 200 ? "w" : "-"),
							(stats["mode"] & 400 ? "x" : "-")
						].join("")
					},
					"type": (stats.isFile && stats.isDirectory)?(stats.isFile()?"file":(stats.isDirectory?"directory":"unknown")):(stats["mode"] & 100000 ? "directory" : (stats["mode"] & 40000 ? "file" : "unknown"))
				});
			}
		});
	});
}

function getDLSCredentialsViaInteractiveLogin(tenantId) {
	return new Promise( (resolve, reject) => {
		msRestAzure.interactiveLogin({
			"domain": tenantId
		},
		function(err, credentials) {
			if (err) {
				return reject(err);
			}
			console.log("Got credentials", credentials);
			setCachedDLSCredentials(credentials);
			return resolve(credentials);
		});
	});
}

function setCachedDLSCredentials(credentials) {
	var cacheFile = './cached-credentials.json';
	return new Promise((resolve, reject) => {
		fs.writeFile(cacheFile, JSON.stringify(credentials), 'utf-8', function(err, response) {
			if(err) {
				return reject(err);
			}
			return resolve(response);
		});
	});
}

function _applyObjectOverAnother(objectOrigin, objectToAdd) {
	Object.keys(objectToAdd).forEach(function(key) {
		if (objectOrigin[key] && (typeof objectOrigin[key]) === "object" && (typeof objectToAdd[key]) === "object") {
			_applyObjectOverAnother(objectOrigin[key], objectToAdd[key]);
		} else {
			objectOrigin[key] = objectToAdd[key];
		}
	})
}

function _transformCredentialsToClass(credentials) {
	if (!msRestAzure.DeviceTokenCredentials) {
		throw new Error("Missing class DeviceTokenCredentials - cannot create credentials");
	}
	var cred = new msRestAzure.DeviceTokenCredentials();
	_applyObjectOverAnother(cred, credentials);
	return cred;
}

async function checkIsCredentialExpired(credentials) {
	return new Promise((resolve, reject) => {
		credentials.getToken(function(err, response) {
			if (err) {
				return reject(err);
			}
			if (!response || !response.expiresOn) {
				return reject(new Error("Invalid response"));
			}
			var expirationDate = new Date(response.expiresOn);
			var now = new Date();
			return resolve((expirationDate - now < 100));
		});
	});
}

async function getCachedDLSCredentials() {
	var cacheFile = './cached-credentials.json';
	if (await file_exists(cacheFile)) {
		return await (new Promise(async (resolve, reject) => {
			fs.readFile(cacheFile, 'utf8', async (err, response) => {
				if (err) {
					return reject(err);
				}
				var credentials = _transformCredentialsToClass(JSON.parse(response));
				if (await checkIsCredentialExpired(credentials)) {
					return resolve(false);
				}
				return resolve(credentials);
			});
		}));
	}
}

async function getDLSCredentials(tenantId, checkCache = true) {
	var credentials;
	if (checkCache) {
		try {
			if (credentials = await getCachedDLSCredentials()) {
				console.log("Getting cached credentials");
				return credentials;
			}
		} catch (err) {
			console.log("Could not get cached file due to error: "+err);
		}
	}
	credentials = await getDLSCredentialsViaInteractiveLogin(tenantId);
	return credentials;
}

function listDLSFiles(fsClient, accountName, path) {
	return new Promise((resolve, reject) => {
		if (!(fsClient instanceof adlsManagement.DataLakeStoreFileSystemClient)) {
			return reject(new Error("Invalid file system client"));
		}
		if (typeof accountName !== "string") {
			return reject(new Error("Invalid account name"));
		}
		if (typeof path !== "string") {
			return reject(new Error("Invalid path"));
		}
		fsClient.fileSystem.listFileStatus(accountName, path, function(err, result, request, response) {
			if (err) {
				return reject(err);
			}
			if (!result || !request) {
				return reject(new Error("Invalid result"));
			}
			if (!result.fileStatuses) {
				return reject(new Error("Could not retrieve status from result"));
			}
			if (!result.fileStatuses.fileStatus) {
				return resolve([]);
			}
			return resolve(result.fileStatuses.fileStatus);
		});
	});
}

function _storeStreamToFile(stream, path) {
	return new Promise(function(resolve, reject) {
		stream.on('error', error => {
			if (stream.truncated)
				fs.unlinkSync(path);
				reject(error);
			})
			.pipe(fs.createWriteStream(path))
			.on('error', error => reject(error))
			.on('finish', () => true)
			.on('close', () => resolve(path));
	});
}

function downloadDLSFile(fsClient, accountName, path, destination) {
	return new Promise(function(resolve, reject) {
		fsClient.fileSystem.open(accountName, path, {}, function(err, result, request, response) {
			_storeStreamToFile(result, destination).then(resolve).catch(reject);
		});
	});
}

function treatByteString(bytes) {
	if (bytes > 1024*1024*1024) {
		return (bytes/(1024*1024*1024)).toFixed(2)+" GB";
	} else if (bytes > 1024*1024) {
		return (bytes/(1024*1024)).toFixed(2)+" MB";
	} else if (bytes > 1024) {
		return (bytes/(1024)).toFixed(2)+" kB";
	} else {
		return (bytes|0).toFixed(0)+" bytes";
	}
}

async function _getFileTime(path, mtime) {
	var info = await stat(path);
	return info["modified-time"];
}

function _setFileTime(path, mtime) {
	return new Promise((resolve, reject) =>
		fs.utimes(path, mtime, mtime, (err, response) => {
			if (err) {
				reject(err);
			}
			return resolve(response);
		})
	);
}

async function recursivelyDownloadDLSFiles(fsClient, accountName, path, destination, depth=0) {
	if (depth > 4 || isNaN(depth)) {
		return console.log("Recursion too deep at \""+path+"\"");
	}

	var elements = await listDLSFiles(fsClient, accountName, path);

	if (elements.length && !(await file_exists(destination))) {
		mkdir(destination);
	}

	var promises = elements.map(async function(element) {
		var internalPath = path+"/"+element.pathSuffix;
		var internalDestination = destination+"/"+element.pathSuffix;
		if (element.type === "DIRECTORY") {
			console.log("Expanding \""+internalPath+"\"...");
			return await recursivelyDownloadDLSFiles(fsClient, accountName, internalPath, internalDestination, depth + 1);
		} else if (element.type === "FILE") {
			if (await file_exists(internalDestination)) {
				var localModifiedTime = new Date(await _getFileTime(internalDestination));
				var modificationTime = new Date(element.modificationTime);
				if (1 >= Math.abs(localModifiedTime-modificationTime)) {
					return console.log("Skipping up to date file \""+internalPath+"\""); // is up to date
				} else {
					//console.log("Exists but new version because its", Math.abs(localModifiedTime-modificationTime));
				}
			}
			if (element.length < 1024*1024*1024) {
				console.log("Downloading \""+internalPath+"\": "+treatByteString(element.length));
				var result = await downloadDLSFile(fsClient, accountName, internalPath, internalDestination);
				if (await file_exists(internalDestination)) {
					var setTimeResult = await _setFileTime(internalDestination, new Date(element.modificationTime));
				}
				return result;
			} else {
				console.log("Skipping large file \""+internalPath+"\": "+treatByteString(element.length));
			}
		} else {
			console.warn("Unknown file type: \""+element.type+"\"");
		}
	});
	var results = await Promise.all(promises.filter(a=>a));
}

module.exports = {
	blobContainerExist, downloadBlobFile, getContainerInfo, downloadBlobContainerFiles,
	stat, mkdir, file_exists,
	getDLSCredentials, checkIsCredentialExpired, listDLSFiles, downloadDLSFile, recursivelyDownloadDLSFiles
}