package org.broadinstitute.hellbender.tools.spark.sv;

import org.broadinstitute.hellbender.exceptions.GATKException;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.broadinstitute.hellbender.tools.spark.sv.SVKmer.Base;

/** iterator over kmers with a specified maximum DUST-style, low-complexity score */
public class SVKmerizerWithLowComplexityFilter extends SVKmerizer {
    private final int maxDUSTScore;
    private final int[] codonCounts;
    private int curDUSTScore;

    public SVKmerizerWithLowComplexityFilter( final byte[] seq, final int kSize, final int maxDUSTScore ) {
        this(new ASCIICharSequence(seq), kSize, maxDUSTScore);
    }

    public SVKmerizerWithLowComplexityFilter( final CharSequence seq, final int kSize, final int maxDUSTScore ) {
        super(kSize, seq);
        if ( kSize < 6 ) {
            throw new GATKException("kmer size must be at least 6 for this filter to work properly.");
        }
        this.maxDUSTScore = maxDUSTScore;
        codonCounts = new int[64]; // there are 64 different codons
        codonCounts[0] = kSize - 2; // the current kmer is poly-A, so codon 0 (AAA) has kSize-2 counts
        curDUSTScore = (kSize-2)*(kSize-3)/2;
        this.nextKmer = nextKmer(new SVKmer(kSize), 0);
    }

    public static Stream<SVKmer> stream( final CharSequence seq, final int kSize, final int maxDUSTScore ) {
        return StreamSupport.stream(((Iterable<SVKmer>)() ->
                new SVKmerizerWithLowComplexityFilter(seq, kSize, maxDUSTScore)).spliterator(), false);
    }

    public static Stream<SVKmer> stream( final byte[] seq, final int kSize, final int maxDUSTScore ) {
        return StreamSupport.stream(((Iterable<SVKmer>)() ->
                new SVKmerizerWithLowComplexityFilter(seq, kSize, maxDUSTScore)).spliterator(), false);
    }

    protected SVKmer nextKmer( SVKmer tmpKmer, int validBaseCount ) {
        final int len = seq.length();
        while ( idx < len ) {
            curDUSTScore -= --codonCounts[tmpKmer.firstCodon(kSize)];
            switch ( seq.charAt(idx) ) {
                case 'a': case 'A':
                    tmpKmer = tmpKmer.successor(Base.A, kSize);
                    break;
                case 'c': case 'C':
                    tmpKmer = tmpKmer.successor(Base.C, kSize);
                    break;
                case 'g': case 'G':
                    tmpKmer = tmpKmer.successor(Base.G, kSize);
                    break;
                case 't': case 'T':
                    tmpKmer = tmpKmer.successor(Base.T, kSize);
                    break;
                default:
                    tmpKmer = tmpKmer.successor(Base.A, kSize);
                    validBaseCount = -1;
                    break;
            }
            curDUSTScore += codonCounts[tmpKmer.lastCodon(kSize)]++;
            idx += 1;

            if ( ++validBaseCount >= kSize && curDUSTScore <= maxDUSTScore ) return tmpKmer;
        }
        return null;
    }
}
