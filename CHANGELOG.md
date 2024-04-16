## 0.4.0

* Allow specifying check-interval higher than 0 together with no limit configuration
* Set default check-interval to 60s
* Change excluded principal definition to be semicolon-separated list

### Changes, deprecations and removals

* **Important:** From 0.4.0 the excluded principals are configured as semicolon-separated list. In previous versions, the principals were
configured using comma-separated list.