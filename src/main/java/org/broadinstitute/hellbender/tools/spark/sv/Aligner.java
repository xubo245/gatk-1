package org.broadinstitute.hellbender.tools.spark.sv;

import java.util.List;

public interface Aligner {
    // return the reference contig names
    List<String> getContigNames();

    // align each contig, and return a list of Alignments for each
    List<List<Alignment>> alignContigs( final List<byte[]> tigSeqs );
}
