# Changelog

## v1.2.1 🌈

### 🐛 Bug Fixes

- Fix infinite loop on callback calling is_scheduled() #37.

## v1.2.0 🌈

### 🚀 Features

- Rename `*Job` models to `*Task` to differentiate.

## v1.1.0 🌈

### 🚀 Features

- Enable using stats view using api token
- Reverted, active jobs are not marked as scheduled as there is currently no new job instance for them.

### 🐛 Bug Fixes

- #32 Running jobs should be marked as scheduled jobs. @rstalbow (#33)

## v1.0.2 🌈

### 🧰 Maintenance

- Update dependencies

### 🐛 Bug Fixes

- Add missing migration and check for missing migrations

## v1.0.1 🌈

* Update dependencies
* Remove redundant log calls

## v1.0.0 🌈

* Migrated from django-rq-scheduler

<!--
### 🚀 Features

### 🐛 Bug Fixes

### 🧰 Maintenance
-->