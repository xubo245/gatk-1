package org.broadinstitute.hellbender.tools.spark.sv;

import htsjdk.samtools.Cigar;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BwaTest extends BaseTest {
    @Test(groups = "spark")
    void loadImageTest() {
        try (BwaMemAligner aligner = new BwaMemAligner(getToolTestDataDir()+"bwamem.img")) {
            Assert.assertEquals(aligner.getContigNames(), Arrays.asList("tig1", "tig2", "tig3", "tig4", "tig5", "tig6"));
        }
    }

    @Test(groups = "spark")
    void alignContigTest() {
        try (BwaMemAligner aligner = new BwaMemAligner(getToolTestDataDir()+"bwamem.img")) {
            List<AlignmentRegion> alignments =
                    aligner.alignContigs(
                        Collections.singletonList("AAACAACACCCAACTGACAGTATTAGACAGATCAGAGAGGCAGAAAATTAATTAACAAAGATATTCAGGACCTGAAGTCAACACTGTGCCAAATGAATGTAATAGATATCTACGTAACTCTCCACCTGAAAACAACAGAGTGTACATTCTTCTCATCTGTACATGGCACATACACTAAAATCAATTT".getBytes()),
                        1);
            Assert.assertEquals(alignments,
                    Arrays.asList(new AlignmentRegion("assembly1","tig0",new Cigar(),true,new SimpleInterval("tig3",288,468),60,4,184,0)));
        }
    }
}
