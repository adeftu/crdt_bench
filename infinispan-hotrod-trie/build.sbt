name := "infinispan-hotrod-trie"

version := "0.1"

parallelExecution in Test := false

unmanagedClasspath in Test += file("etc")