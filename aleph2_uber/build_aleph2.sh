#!/bin/bash 
# Build file for building all aleph2 core and contrib, the order is important here.
# create a link in ~/bin to this file e.g. ln -s ~/git/Aleph2/aleph2_uber/build_aleph2.sh ~/bin/build_aleph2.sh
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

cd ~/git/Aleph2-contrib
mvn -e install -DskipTests=true -Daleph2.scope=provided


