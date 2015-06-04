# twittermpi
Application of natural processing of tweets in OpenMPI

Tested with MPI version 1.8.4. (open-mpi) 

To compile:

    mvn clean package assembly:single --mpi and juniper monitoring modules must be installed to local maven repository

To install mpi.jar to local maven repository:

    mvn install:install-file -Dfile=/path/to/mpi.jar -DgroupId=mpi -DartifactId=MPI -Dversion=1.8.4 -Dpackaging=jar

To install juniper-sa-monitoring-agent-0.1-SNAPSHOT.jar to local maven repository:

    mvn install:install-file -Dfile=/path/to/juniper-sa-monitoring-agent-0.1-SNAPSHOT.jar -DgroupId=eu.juniper.sa.monitoring -DartifactId=juniper-sa-monitoring-agent -Dversion=0.1-SNAPSHOT -Dpackaging=jar

Application expects mounted ram directory at:

    /ram
    
where it stores monitoring data

