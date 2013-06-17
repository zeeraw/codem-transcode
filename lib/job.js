var util          = require('util'),
    crypto        = require('crypto'),
    fs            = require('fs'),
    child_process = require('child_process'),
    config        = require('./config').load()
    mkdirp        = require('mkdirp'),
    path          = require('path'),
    notifyHandler = require('./notify-handler'),
    logger        = require('./logger'),
    Sequelize     = require('sequelize'),
    http          = require('http'),
    https         = require('https'),
    mime          = require('mime');

var aws
if (config['aws_key'] != undefined) {
  try {
    aws = require("aws-sdk")
    aws.config.update(
      {
        accessKeyId:     config['aws_key'],
        secretAccessKey: config['aws_secret'],
        region:          config['aws_region']
      }
    );
  } catch (err) {
    console.log("You need to install aws-sdk package manually");
  }
}

var StatusCodes = {
  SUCCESS: "success",
  FAILED: "failed",
  PROCESSING: "processing"
}

var JobUtils = {
  sql: null,

  getDatabase: function() {
    if (JobUtils.sql == null) {
      if (config['database']['dialect'] == "sqlite") {
        JobUtils.sql = new Sequelize('database', 'username', 'password', {
          dialect: 'sqlite',
          storage: config['database']['database'],
          logging: false
        });
      } else {
        JobUtils.sql = new Sequelize(config['database']['database'], config['database']['username'], config['database']['password'], {
          dialect: config['database']['dialect'],
          storage: config['database']['database'],
          host: config['database']['host'],
          logging: false
        });
      }
    }
    return JobUtils.sql;
  },

  getMask: function() {
    return JobUtils.pad((process.umask() ^ 0777).toString(8), '0', 4);
  },

  markUnknownState: function(callback) {
    Job.findAll({ where: { status: StatusCodes.PROCESSING }}).success(function(result) {
      if (result.length > 0) {
        for (var job in result) {
          result[job].status = StatusCodes.FAILED;
          result[job].message = "The transcoder quit unexpectedly or the database was unavailable for some period.";
          result[job].save();
        }
      }
    });
    callback(null);
  },

  migrateDatabase: function(callback) {
    var migrator = JobUtils.getDatabase().getMigrator({path: __dirname + "/../migrations" });
    migrator.migrate().success(function() {
      callback(null);
    }).error(function(err) {
      callback(err);
    });
  },

  pad: function(orig, padString, length) {
    var str = orig;
    while (str.length < length)
        str = padString + str;
    return str;
  },

  verifyDatabase: function(callback) {
    JobUtils.getDatabase().getQueryInterface().showAllTables().success(function (tables) {
      if (tables.length > 0 && tables.indexOf('SequelizeMeta') == -1) {
        logger.log("You appear to be upgrading from an old version of codem-transcode (<0.5). The database handling has " +
                   "changed, please refer to the upgrade instructions. To prevent data loss the transcoder will now exit.");
        callback("Old database schema detected.");
      } else {
        callback(null);
      }
    }).error(function (err) {
      logger.log(err);
      callback(err);
    });
  }
}

// Model definition
var Job = JobUtils.getDatabase().define('Job', {
  id:          { type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true },
  internalId:  { type: Sequelize.STRING, defaultValue: null },
  status:      { type: Sequelize.STRING, defaultValue: StatusCodes.PROCESSING },
  progress:    { type: Sequelize.FLOAT, defaultValue: 0.0 },
  duration:    { type: Sequelize.INTEGER, defaultValue: 0 },
  filesize:    { type: Sequelize.INTEGER, defaultValue: 0 },
  opts:        { type: Sequelize.TEXT, defaultValue: null },
  message:     { type: Sequelize.TEXT, defaultValue: null },
  createdAt:   Sequelize.DATE,
  updatedAt:   Sequelize.DATE
}, {
  classMethods: {
    prepareDatabase: function(callback) {
      JobUtils.verifyDatabase(function (err) {
        if (err) {
          callback(err);
        } else {
          JobUtils.migrateDatabase(function (err) {
            if (err) {
              callback(err);
            } else {
              JobUtils.markUnknownState(function (err) {
                err ? callback(err) : callback(null);
              })
            }
          });
        }
      })
    },
    create: function(opts, callback, completeCallback) {
      var job = Job.build({ opts: JSON.stringify(opts), internalId: Job.generateId() });
      job.save().success(function(job) {
        job.prepare(function onPrepared(message,type) {
          if (message == "hasInput")     job.hasInput     = true;
          if (message == "hasOutput")    job.hasOutput    = true;
          if (message == "hasThumb")     job.hasThumb     = true;
          if (message == "hasInput")     job.inputType    = type;
          if (message == "hasOutput")    job.outputType   = type;
          if (message == "hasThumb")     job.thumbType    = type;
          if (job.hasInput && job.hasOutput) job.spawn();
        })
        job.completeCallback = completeCallback;
        callback(null, job);
      }).error(function(error) {
        // Error while saving job, this appears to not always be called(!)
        logger.log("Could not write job " + job.internalId + " to the database.");
        callback('Unable to save new job to the database', null);
      });
    },
    generateId: function() {
      var hash = crypto.createHash('sha1');
      var date = new Date();

      hash.update([date, Math.random()].join(''));

      return hash.digest('hex');
    }
  },
  instanceMethods: {
    parsedOpts: function() {
      return JSON.parse(this.opts);
    },

    prepare: function(callback) {
      var job = this;
      var mode = JobUtils.getMask();

      var destination_dir,
          source_dir,
          destination_type,
          source_type,
          thumbnail_destination_type,
          thumbnail_destination_dir;

      if (job.parsedOpts()['source_file'].match(/^https?:\/\/(.+)$/i)) {
        source_type = "http"
      } else {
        source_type = "file"
      }

      switch(source_type) {

        case "http":
          source_dir = config['scratch_dir'] + "/input/"

          mkdirp.mkdirp(source_dir, mode, function(err) {
            if (err) {
              fs.stat(source_dir, function(err, stats) {
                if (err) {
                  job.exitHandler(-1, "unable to create input directory (" + source_dir + ").");
                } else {
                  if (stats.isDirectory()) {
                    callback("hasInput",source_type);
                  } else {
                    job.exitHandler(-1, "unable to create input directory (" + source_dir + ").");
                  }
                }
              });
            } else {
              callback("hasInput",source_type);
            }
          });
          break;

        case "file":
          fs.stat(job.parsedOpts()['source_file'], function(err, stats) {
            if (err) {
              job.exitHandler(-1, "unable to read input file (" + job.parsedOpts()['source_file'] + ").");
            } else {
              if (stats.isFile()) { job.filesize = stats.size || Number.NaN };
              callback("hasInput",source_type);
            }
          });
          break;

      }

      if (job.parsedOpts()['thumbnail_destination_file']) {
        if (job.parsedOpts()['thumbnail_destination_file'].match(/^s3:\/\/(.+)$/)) {
          thumbnail_destination_type = "s3"
        } else {
          thumbnail_destination_type = "file"
        }

        switch(thumbnail_destination_type) {

          case "s3":
            thumbnail_destination_dir = config['scratch_dir'] + "/thumbnails/"
            break;

          case "file":
            thumbnail_destination_dir = path.dirname(
              path.normalize(
                job.parsedOpts()['thumbnail_destination_file']
              )
            );
            break;
        }

        mkdirp.mkdirp(thumbnail_destination_dir, mode, function(err) {
          if (err) {
            fs.stat(thumbnail_destination_dir, function(err, stats) {
              if (err) {
                job.exitHandler(-1, "unable to create thumbnail directory (" + thumbnail_destination_dir + ").");
              } else {
                if (stats.isDirectory()) {
                  callback("hasThumb",thumbnail_destination_type);
                } else {
                  job.exitHandler(-1, "unable to create thumbnail directory (" + thumbnail_destination_dir + ").");
                }
              }
            });
          } else {
            callback("hasThumb",thumbnail_destination_type);
          }
        });
      } else {
        job.hasThumb  = false
        job.thumbType = undefined
      }


      if (job.parsedOpts()['destination_file'].match(/^s3:\/\/(.+)$/)) {
        destination_type = "s3"
      } else {
        destination_type = "file"
      }

      switch(destination_type) {

        case "s3":
          destination_dir = config['scratch_dir'] + "/output/"
          break;

        case "file":
          destination_dir = path.dirname(
            path.normalize(
              job.parsedOpts()['destination_file']
            )
          );
          break;
      }

      mkdirp.mkdirp(destination_dir, mode, function(err) {
        if (err) {
          fs.stat(destination_dir, function(err, stats) {
            if (err) {
              job.exitHandler(-1, "unable to create output directory (" + destination_dir + ").");
            } else {
              if (stats.isDirectory()) {
                callback("hasOutput",destination_type);
              } else {
                job.exitHandler(-1, "unable to create output directory (" + destination_dir + ").");
              }
            }
          });
        } else {
          callback("hasOutput",destination_type);
        }
      });

    },
    spawn: function() {
      var job = this;
      if (this.hasInput && this.hasOutput && this.hasThumb && !this.the_process) {

        // INPUT PATH CONFIG
        switch(this.inputType) {
          case "http":
            var sourceFileExt   = path.extname(this.parsedOpts()['source_file']);
            var sourceFileName  = this.parsedOpts()['source_file'].match(/.+\/([^\/]+$)/)[1]
            if (sourceFileExt.length <= 0) sourceFileExt = ".mp4";
            this.sourceFilePath = config['scratch_dir'] + "/input/" + this.id + sourceFileExt;
            break;
          case "file":
            this.sourceFilePath = this.parsedOpts()['source_file']
            break;
        }

        // THUMBNAIL DESTINATION CONFIG
        var extension = path.extname(this.parsedOpts()['thumbnail_destination_file']);
        if (extension.length <= 0) extension = ".png";

        switch(this.thumbType) {
          case "s3":
            this.thumbnailDestinationFilePath = config['scratch_dir'] + "/thumbnails/" + this.id + extension;
            break;

          case "file":
            if (config['use_scratch_dir'] == true) {
              this.thumbnailDestinationFilePath = config['scratch_dir'] + "/thumbnails/" + this.id + extension;
            } else {
              var outputDir = path.dirname(this.parsedOpts()['thumbnail_destination_file']);
              this.thumbnailDestinationFilePath = outputDir + "/" + this.id + extension;
            }
            break;

        }

        // OUTPUT DESTINATION CONFIG
        var extension = path.extname(this.parsedOpts()['destination_file']);
        if (extension.length <= 0) extension = ".mp4";

        switch(this.outputType) {
          case "s3":
            var destFileName = this.parsedOpts()['destination_file'].match(/^(s3:\/\/)([a-z0-9\.\_\-]+)\/(.+)$/i)[3]
            this.destinationFilePath = config['scratch_dir'] + "/output/" + this.id + extension;
            break;

          case "file":
            if (config['use_scratch_dir'] == true) {
              this.destinationFilePath = config['scratch_dir'] + "/output/" + this.id + extension;
            } else {
              var outputDir = path.dirname(this.parsedOpts()['destination_file']);
              this.destinationFilePath = outputDir + "/" + this.id + extension;
            }
            break;

        }

        if (this.inputType == "http") {

          var downloadFile    = fs.createWriteStream(this.sourceFilePath);

          if (this.parsedOpts()["source_file"].match(/^https/i)) {

            logger.log('downloading the source file (' + this.parsedOpts()["source_file"] + ').');

            req = https.get(this.parsedOpts()["source_file"], function(res) {
              res.pipe(downloadFile);
              res.on("data", function(chunk) {
                // handle progress
              });
              res.on("end", function(e) {
              job.probe();
              });
            }).on("error", function(e) {
              this.exitHandler(-1, "unable to download file (" + this.parsedOpts()["source_file"] + ").");
            });

          } else {
            // do a non-ssl http request
          }

        } else {

        }

      }
    },
    transcode: function() {
      var job = this;

      var transcodeArgs = []

      transcodeArgs.push('-i', job.sourceFilePath);

      job.parsedOpts()['encoder_options'].replace(/\s+/g, " ").split(' ').forEach(function(item){
        transcodeArgs.push(item)
      });

      transcodeArgs.push(job.destinationFilePath);

      if (job.parsedOpts()['thumbnail_destination_file']) {
        var dur = parseInt(job.inputFileMeta['duration']) / 2
        transcodeArgs.push("-ss")
        transcodeArgs.push(dur)
        job.parsedOpts()['thumbnail_options'].replace(/\s+/g, " ").split(' ').forEach(
          function(item){ transcodeArgs.push(item)
        });
        transcodeArgs.push(job.thumbnailDestinationFilePath);
      }


      var the_process = child_process.spawn(config['encoder'], transcodeArgs);

      the_process.stderr.on('data', function(data) { job.progressHandler(data) });
      the_process.on('exit', function(code) {
        job.finalize(code, job.destinationFilePath, job.outputType, job.parsedOpts()['destination_file']);
        if (job.parsedOpts()['thumbnail_destination_file']) {
          job.finalize(code, job.thumbnailDestinationFilePath, job.thumbType, job.parsedOpts()['thumbnail_destination_file']);
        }
      });

      this.the_process = the_process;

    },
    probe: function() {

      var job = this;

      var probeArgs = "-show_streams".replace(/\s+/g, " ").split(' ');
      probeArgs.push(job.sourceFilePath);

      var the_process = child_process.spawn("ffprobe", probeArgs);
      var outputBuffer;

      var inputFileMeta = {};

      the_process.stdout.on('data', function(data) {
        if (outputBuffer) { outputBuffer = Buffer.concat([outputBuffer,data]); } else { outputBuffer = data }
      });

      the_process.on('exit', function(code) {

        var probeDataSane = outputBuffer.toString('utf-8').split('\n').filter(
          function(val) {
            return val.match(/[a-zA-Z\:]+\=.+/i);
          }
        );

        probeDataSane.forEach(function(item) {
          var keyValue = item.split("=");
          inputFileMeta[keyValue[0]] = keyValue[1];
        });

        job.transcode();

      });

      this.the_process = the_process;
      this.inputFileMeta = inputFileMeta;
    },
    cancel: function() {
      if (this.the_process) {
        this.the_process.kill();
        this.exitHandler(-1, 'job was cancelled');
      }
    },
    finalize: function(code, tmpFile, outputType, dest) {
      var job = this;
      if (code == 0) {
        switch(outputType) {
          case "s3":
            var destination = dest.match(/^(s3:\/\/)([a-z0-9\.\_\-]+)\/(.+)$/)
            var bucket      = destination[2]
            var filename    = destination[3]

            try {
              logger.log('ffmpeg finished successfully, trying to upload file (' + filename + ') to s3 bucket (' + bucket + ').');

              fs.readFile(tmpFile, function (err, data) {
                if (err) { throw err; }

                var s3         = new aws.S3();
                var filebody   = new Buffer(data, 'binary');
                var mimetype   = mime.lookup(tmpFile)

                var request = s3.client.putObject({
                  Bucket: bucket,
                  Key:    filename,
                  Body:   filebody,
                  ContentType: mimetype
                });

                request.on("success", function (resp) {
                  job.exitHandler(code, 'ffmpeg finished succesfully.');
                });

                request.on("error", function (resp) {
                  logger.log(resp);
                  job.exitHandler(-1, 'ffmpeg finished succesfully, but failed midway uploading the file (' + filename + ') to s3 bucket (' + bucket + ').');
                });

                request.send();

              });
            } catch (err) {
              logger.log(err);
              job.exitHandler(-1, 'ffmpeg finished succesfully, but unable to upload file (' + filename + ') to s3 bucket (' + bucket + ').');
            }
            break;

          case "file":
            fs.rename(tmpFile, dest, function (err) {
              if (err) {
                if ( (err.message).match(/EXDEV/) ) {
                  /*
                    EXDEV fix, since util.pump is deprecated, using stream.pipe
                    example from http://stackoverflow.com/questions/11293857/fastest-way-to-copy-file-in-node-js
                  */
                  try {
                    logger.log('ffmpeg finished successfully, trying to copy across partitions');
                    fs.createReadStream(tmpFile).pipe(fs.createWriteStream(dest));
                    job.exitHandler(code, 'ffmpeg finished succesfully.');
                  } catch (err) {
                    logger.log(err);
                    job.exitHandler(-1, 'ffmpeg finished succesfully, but unable to move file to different partition (' + dest + ').');
                  }
                }

              } else {
                job.exitHandler(code, 'ffmpeg finished succesfully.');
              }
            });
            break;

          default:
            job.exitHandler(-1, 'ffmpeg finished succesfully, but unable to move file to destination (' + dest + ').');
            break;
        }
      } else {
        job.exitHandler(code, "ffmpeg finished with an error: '" + job.lastMessage + "' (" + code + ").")
      }
    },
    toJSON: function() {
      var obj = {
        'id': this.internalId,
        'status': this.status,
        'progress': this.progress,
        'duration': this.duration,
        'filesize': this.filesize,
        'message': this.message
      };

      return obj;
    },
    progressHandler: function(data) {
      this.lastMessage = data.toString().replace("\n",'');

      (isNaN(this.duration) || this.duration == 0) ? this.extractDuration(data.toString()) : this.extractProgress(data.toString());

      this.save().success(function() {
        // successfull save
      }).error(function(err) {
        // error while saving job
      });
    },
    extractDuration: function(text) {
      if (!this.durationBuffer) this.durationBuffer = "";

      this.durationBuffer += text;
      var re = new RegExp(/Duration:\s+(\d{2}):(\d{2}):(\d{2}).(\d{1,2})/);
      var m = re.exec(this.durationBuffer);

      if (m != null) {
        var hours = parseInt(m[1], 10), minutes = parseInt(m[2], 10), seconds = parseInt(m[3], 10);

        this.duration = hours * 3600 + minutes * 60 + seconds;
        notifyHandler.notify(this);
      }
    },
    extractProgress: function(text) {
      // 00:00:00 (hours, minutes, seconds)
      var re = new RegExp(/time=(\d{2}):(\d{2}):(\d{2})/);
      var m = re.exec(text);

      if (m != null) {
        var hours = parseInt(m[1], 10), minutes = parseInt(m[2], 10), seconds = parseInt(m[3], 10);
        var current = hours * 3600 + minutes * 60 + seconds;
        this.progress = current / this.duration;
      } else {
        // 00.00 (seconds, hundreds)
        re = new RegExp(/time=(\d+).(\d{2})/);
        m = re.exec(text);

        if (m != null) {
          var current = parseInt(m[1], 10);
          this.progress = current / this.duration;
        }
      }
    },
    exitHandler: function(code, message) {
      if (!this.hasExited) {
        this.hasExited = true
        this.status = (code == 0 ? 'success' : 'failed');
        this.message = message;

        if (this.status == 'success' && !isNaN(this.progress)) {
          this.progress = 1.0;
        }

        this.save();
        this.completeCallback();
      }
    }
  }
});

module.exports = Job;
