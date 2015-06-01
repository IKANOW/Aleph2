#!/bin/bash
cd ~/git/Aleph2/aleph2_core_distributed_services_library
mvn -e install -DskipTests=true -Dmaven.compiler.verbose=true
cd ~/git/Aleph2/aleph2_data_model
mvn -e install -DskipTests=true -Dmaven.compiler.verbose=true
cd ~/git/Aleph2-contrib
mvn -e install -DskipTests=true -Dmaven.compiler.verbose=true
cd ~/git/Aleph2
mvn -e install -DskipTests=true -Dmaven.compiler.verbose=true
# build all single uber jars but using provided aleph2 modules,, so we only inlcude each modules aleph2 code
cd ~/git/Aleph2
mvn -e package -DskipTests=true -Daleph2.scope=provided


