# Version of kafka-connector-source-amq, single source of truth: it is stamped
# into the jar's version resource, the Maven coordinates and the plugin zip.
#
# Releasing = bump this, merge, then tag the merge commit:
#   git tag kafka-connector-source-amq-v<VERSION> && git push origin <tag>
# The release workflow refuses tags that do not match this value.
VERSION = "1.0.0"
