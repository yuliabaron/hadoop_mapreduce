# hadoop_mapreduce

This library produces list of top 10 similar sites for any given site. Similarity is calculated based on the common tags for the given two sites

#### Input:
List of pairs, where each pair is presented by site and tag. In each pair site comes first, followed by its tag. The two are separated by tab. Each pair comes on a new line

#### Assumptions:
- Input pairs are unique

#### ENV for compilation:
$HADOOP_HOME = /home/ubuntu/hadoop-3.1.1/
$CLASSPATH = $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.1.1.jar:$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-common-3.1.1.jar:$HADOOP_HOME/share/hadoop/common/hadoop-common-3.1.1.jar:/home/ubuntu/IdeaProjects/SimilarSites/*:$HADOOP_HOME/lib/*

#### Output:
Lines of triples:
- Each triple is presented by the site, its similar site and the number of common tags, separated by tab
- Up to 10 lines per site
- Lines of the same site are groupped together and sorted in descending order by the common tags number
- If a site has no similar sites it will not appear in the output

#### Algorithm:
The work is done with 3 jobs:
- Job1 (TagToSite) takes the input and transforms it into pairs of sites sharing the same tag, where each pair stands for one common tag
- Job2 (SiteToSiteCount) counts the number of common tags per pair
- Job3 (TopSimilarSites) groups the pairs by the first site in pair, and sorts them in descending order by number of common tags

