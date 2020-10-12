[![pipeline status](https://git.naspersclassifieds.com/shared-services/data-science/data-toolz/badges/master/pipeline.svg)](https://git.naspersclassifieds.com/shared-services/data-science/data-toolz/-/commits/master)
[![coverage report](https://git.naspersclassifieds.com/shared-services/data-science/data-toolz/badges/master/coverage.svg)](https://git.naspersclassifieds.com/shared-services/data-science/data-toolz/-/commits/master)

data-toolz
==========
This repository contains reusable python code for OLX data projects


installation
============
To use this package you need a personal [gitlab access token](https://git.naspersclassifieds.com/profile/personal_access_tokens)

```
pip install data-toolz --index-url https://<your-username>:<your-access-token>@git.naspersclassifieds.com/api/v4/projects/6797/packages/pypi/simple
```

In case you want to use this package as a dependence in a CI/CD pipeline you also need to use an access token.
Depending on the current gitlab version you need
* create your own keys (GitLab <13.4)
* use [CI/CD token](https://docs.gitlab.com/ee/user/packages/pypi_repository/#using-gitlab-ci-with-pypi-packages) (GitLab >= 13.4)
