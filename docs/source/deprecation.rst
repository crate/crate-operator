Deprecated features and APIs
============================

The following page lists all deprecated features and changes to the CrateDB
Kubernetes Operator. Users are encouraged to move away from deprecated features
to ensure a stable an reliable experience.

1.0
---

* Before version 1.0, the operator used Kopf's
  ``kopf.zalando.org/last-handled-configuration``, ``kopf.zalando.org/*``, and
  ``kopf.zalando.org/KopfFinalizerMarker`` annotations and finalizers to track
  a resource's progress and state. These were deprecated in version 1.0 and
  will be removed in 2.0. To upgrade from a version before 1.0 to 2.0 or later,
  one *must* upgrade to 1.0 first.
