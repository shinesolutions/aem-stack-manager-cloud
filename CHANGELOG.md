# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased
### Removed
- Removed preview config parameter `min-preview-publish-instances`, as if we want to proceed with offline-snapshot even if no preview instance is found.

## 2.0.0 - 2023-07-28

### Added
- Added test cases for aem_offline_snapshopt

### Changed
- Updated Offline Snapshot to support offline backup & offline compaction of preview architecture
- Updated Makefile target `ci` to run tests
- Improved DDB Client error logging #50

## 1.9.1 - 2020-11-16
### Fixed
- Fixed naming of suspended processes while aem_offline_snapshot lambda is running

## 1.9.0 - 2020-11-13
### Added
- Add new process to suspend/resume ASG process `AZRebalance` while aem_offline_snapshot lambda is running [#45]

## 1.8.0 - 2020-03-06
### Changed
- Add `AemId` tag filter when selecting snapshots for purge in order to ensure the clean up of AEM-only snapshots and not other application's

## 1.6.0 - 2020-02-28
### Added
- Add ssm parameter `executionTimeout` to aem_offline_snapshot lambda

## 1.5.0 - 2019-10-07
### Added
- Add new process to suspend/resume ASG process `AlarmNotification` while aem_offline_snapshot lambda is running [shinesolutions/aem-aws-stack-builder#295]

### Fixed
- Fix print of wrong instance count while updating publish-dispatcher ASG.

## 1.4.0 - 2019-09-17
### Added
- Add new lambda function for streaming CloudWatch logs to S3

## 1.3.5 - 2019-08-05
### Fixed
- Fixed passing of wrong parameter type on offline-snapshot [#37]

## 1.3.4 - 2019-08-04
### Fixed
- Fixed ec2 filter for getting AEM Author-Standby instance [#35]

## 1.3.3 - 2019-05-19
### Changed
- Lock down dependencies version

## 1.3.2 - 2019-01-24
### Changed
- Handle inexistent author-standby component during offline-snapshot/offline-compaction-snapshot [#28]

## 1.3.1 - 2018-09-11
### Added
- Add additional log messages for the Offline Snapshot Lambda function [#29]

## 1.3.0 - 2018-06-29
### Changed
- Rename offline-snapshot, offline-compaction-snapshot with full-set suffix

### Removed
- Remove unused scripts/manage_document_permission.py

## 1.2.3 - 2018-05-01
### Changed
- Handle existing escaped backslashes in package filter

## 1.2.2 - 2018-04-11
### Changed
- Export backup package filter no longer needs to be escaped

## 1.2.1 - 2018-03-20
### Changed
- Update AEM Stack Manager Lambda function to improve flexibility

## 1.2.0 - 2018-03-08
### Added
- Add new command flush-dispatcher-cache

## 1.1.0 - 2018-03-03
### Added
- Add missing message_id parameter from offline snapshot AEM publish start/stop
- Add aemid parameter in order to handle multi AEM instances on the same server

### Changed
- Fix offline snapshot message payload
- Change artifact type to zip

## 1.0.0 - 2017-06-08
### Added
- Initial version
