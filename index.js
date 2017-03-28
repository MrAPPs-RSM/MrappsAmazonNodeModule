'use strict';

var AWS = require('aws-sdk');

var AwsHelperFactory = function (settings, dbManager) {

    var sequelize = dbManager.db.sequelize;

    var s3 = new AWS.S3({
        region: settings.get('aws').region
    });

    var getEtagInfoFromAWS = function (params) {

        return new Promise(function (resolve) {

            let key = params.key || "";
            let bucket = getCorrectBucket(params);

            console.log('Retrieving etag information for "'+bucket+':'+key+'" from AWS...');

            s3.headObject({Bucket: bucket, Key: key}, function (err, data) {

                if(err) {
                    resolve({etag: null, updatedAt: null});
                    return;
                }

                let etag = data.ETag.replace(/"/g, '');
                let now = new Date();

                dbManager.db.MrappsAmazonS3Objects
                    .findOrCreate({where: {s3Key: key}, defaults: {
                        etag: etag,
                        updatedAt: now
                    }})
                    .spread(function(obj) {

                        if(obj.etag != etag || null === obj.updatedAt) {
                            obj.update({
                                etag: etag,
                                updatedAt: now
                            }).then(function() {
                                resolve({etag: etag, updatedAt: obj.updatedAt});
                            });
                        }else {
                            resolve({etag: etag, updatedAt: obj.updatedAt});
                        }
                    });
            });
        });
    };

    var getCorrectBucket = function (params) {
        var bucket = params.bucket || "";
        if (bucket.length == 0) bucket = settings.get('aws').default_bucket;
        return bucket;
    };
    
    var awsHelper = {};

	/**
	 * Get the public URL of the given object.
	 *
	 * @param  {String} key
	 * @param  {String} bucket (optional)
	 * @return {String}
	 */
    awsHelper.getObjectUrl = function (params) {

        var key = params.key || "";
        var bucket = getCorrectBucket(params);

        return "http://" + bucket + ".s3.amazonaws.com/" + key;
    };

	/**
	 * Get the ETAG of the given object.
	 *
	 * @param  {String} key
	 * @return {String}
	 */
    awsHelper.getEtagInfo = function (params) {

        var key = params.key || "";

        return sequelize.query("SELECT etag, updated_at FROM mrapps_amazon_s3_objects WHERE s3_key = :key",
            {replacements: {key: key}, type: sequelize.QueryTypes.SELECT}
        ).then(function (result) {

            //L'etag l'ha trovato su database?
            let etag = (result.length > 0 && "etag" in result[0]) ? result[0].etag : null;
            let updatedAt = (result.length > 0 && "updated_at" in result[0]) ? result[0].updated_at : null;

            if (etag !== null && etag.length > 0) {
                return Promise.resolve({
                    etag: etag,
                    updatedAt: updatedAt
                });
            }

            //Se non l'ha trovato faccio la chiamata a AWS
            return getEtagInfoFromAWS(params);

        }).catch(function () {

            //Errore? Chiamo AWS
            return getEtagInfoFromAWS(params);
        });
    };

    return awsHelper;
};

module.exports = AwsHelperFactory;