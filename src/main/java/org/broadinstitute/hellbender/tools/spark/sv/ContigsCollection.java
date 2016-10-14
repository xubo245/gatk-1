package org.broadinstitute.hellbender.tools.spark.sv;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.exceptions.GATKException;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Represents a collection of assembled contigs (not including the variants) produced by "sga assemble".
 */
@VisibleForTesting
final class ContigsCollection implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LogManager.getLogger(ContigsCollection.class);

    @VisibleForTesting
    static final class ContigSequence implements Serializable{
        private static final long serialVersionUID = 1L;

        private final String sequence;
        public ContigSequence(final String sequence){ this.sequence = sequence; }

        @Override
        public String toString(){
            return sequence;
        }
    }

    @VisibleForTesting
    static final class ContigID implements Serializable{
        private static final long serialVersionUID = 1L;

        private final String id;
        public ContigID(final String idString) { this.id = idString; }

        @Override
        public String toString(){
            return id;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final ContigID contigID = (ContigID) o;
            return Objects.equals(id, contigID.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    private final List<Tuple2<ContigID, ContigSequence>> contents;

    public List<Tuple2<ContigID, ContigSequence>> getContents(){
        return contents;
    }

    @Override
    public String toString(){
        return StringUtils.join(toListOfStrings(),"\n");
    }

    /**
     * Pack the entire fasta record on one line so that it's easy for downstream
     * Spark tools to parse without worrying about partitioning. If the ContigCollection
     * is empty this returns null.
     */
    public String toPackedFasta(){
        return StringUtils.join(toListOfStrings(),"|");
    }

    /**
     * To ease file format difficulties
     * @param packedFastaLine
     */
    @VisibleForTesting
    protected static ContigsCollection fromPackedFasta(final String packedFastaLine) {
        final List<String> fileContents = Arrays.asList(packedFastaLine.split("\\|"));
        if (fileContents.size() % 2 != 0) {
            throw new GATKException("Odd number of lines in breakpoint fasta" + packedFastaLine);
        }
        return new ContigsCollection(fileContents);
    }

    public List<String> toListOfStrings(){
        if(null==contents){
            return null;
        }
        final List<String> res = new ArrayList<>();
        for(final Tuple2<ContigID, ContigSequence> contig : contents){
            res.add(contig._1().toString());
            res.add(contig._2().toString());
        }
        return res;
    }

    public ContigsCollection(final List<String> fileContents){

        if(null==fileContents){
            contents = null;
        }else{
            final int sz = fileContents.size()/2;
            contents = new ArrayList<>(sz);
            for(int i=0; i<sz; ++i){
                contents.add(new Tuple2<>(new ContigID(fileContents.get(2*i)), new ContigSequence(fileContents.get(2*i+1))));
            }
        }
    }

    public ContigsCollection( final int sz ) {
        contents = new ArrayList<>(sz);
    }

    /**
     * Loads an RDD of {@link ContigsCollection} objects keyed by assembly ID from disk. The input file
     * should be the output of as RunSGAViaProcessBuilderOnSpark.
     */
    static JavaPairRDD<String, ContigsCollection> loadContigsCollectionKeyedByAssemblyId(final JavaSparkContext ctx, final String inputPath) {
        final JavaRDD<String> inputAssemblies = ctx.textFile(inputPath).cache();

        final JavaPairRDD<String, String> contigCollectionByBreakpointId =
                inputAssemblies
                        .flatMapToPair(ContigsCollection::splitAssemblyLine);

        return contigCollectionByBreakpointId.mapValues(ContigsCollection::fromPackedFasta);
    }

    /**
     * input format is the text representation of an alignment region
     * @param alignedAssembledContigLine An input line with the tab-separated fields of an alignment region
     * @return A tuple with the breakpoint ID and string representation of an BreakpointAlignment, or an empty iterator if the line did not have two comma-separated values
     */
    static AlignmentRegion parseAlignedAssembledContigLine(final String alignedAssembledContigLine) {
        final String[] split = alignedAssembledContigLine.split("\t", -1);
        return AlignmentRegion.fromString(split);
    }

    /**
     * input format is tab separated BreakpointId, PackedFastaFile
     * @param assemblyLine An input line with a breakpoint ID and packed FASTA line
     * @return A tuple with the breakpoint ID and packed FASTA line, or an empty iterator if the line did not have two tab-separated values
     */
    static Iterable<Tuple2<String, String>> splitAssemblyLine(final String assemblyLine) {

        final String[] split = assemblyLine.split("\t");
        if (split.length < 2) {
            logger.info("No assembled contigs for breakpoint " + split[0]);
            return Collections.emptySet();
        }
        return Collections.singleton(new Tuple2<>(split[0], split[1]));
    }

}
