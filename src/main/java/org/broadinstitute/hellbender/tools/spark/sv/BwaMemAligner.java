package org.broadinstitute.hellbender.tools.spark.sv;

import com.google.common.annotations.VisibleForTesting;
import htsjdk.samtools.Cigar;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.NativeUtils;
import org.broadinstitute.hellbender.utils.SimpleInterval;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class BwaMemAligner implements AutoCloseable {
    private long indexAddress;
    private final List<String> refContigNames;

    static {
        final String libName;
        if (NativeUtils.runningOnLinux()) libName = "/libbwa.Linux.so";
        else if (NativeUtils.runningOnMac()) libName = "/libbwa.Darwin.dylib";
        else libName = null;
        if ( libName == null ) {
            throw new UserException.HardwareFeatureException("We have a JNI binding for bwa-mem only for Linux and Mac.");
        }
        if ( !NativeUtils.loadLibraryFromClasspath(libName) ) {
            throw new UserException.HardwareFeatureException("Misconfiguration: Unable to load bwa-mem native library "+libName);
        }
    }

    public BwaMemAligner( final String indexImageFile ) {
        indexAddress = createIndex(indexImageFile);
        if ( indexAddress == 0L ) {
            throw new GATKException("Unable to open bwa-mem index "+indexImageFile);
        }
        ByteBuffer refContigNamesBuf = getRefContigNames(indexAddress);
        if ( refContigNamesBuf == null ) {
            throw new GATKException("Unable to retrieve reference contig names from bwa-mem index "+indexImageFile);
        }
        refContigNamesBuf.order(ByteOrder.nativeOrder()).position(0).limit(refContigNamesBuf.capacity());
        int nRefContigNames = refContigNamesBuf.getInt();
        refContigNames = new ArrayList<>(nRefContigNames);
        for ( int idx = 0; idx < nRefContigNames; ++idx ) {
            int nameLen = refContigNamesBuf.getInt();
            byte[] nameBytes = new byte[nameLen];
            refContigNamesBuf.get(nameBytes);
            refContigNames.add(new String(nameBytes));
        }
        destroyRefContigNames(refContigNamesBuf);
    }

    @Override
    public void close() {
        if ( indexAddress != 0 ) destroyIndex(indexAddress);
        indexAddress = 0;
    }

    @VisibleForTesting List<String> getContigNames() { return refContigNames; }

    public List<AlignmentRegion> alignContigs( List<byte[]> contigSequences, int assemblyId ) {
        if ( indexAddress == 0L ) {
            throw new GATKException("No bwa-mem index is open.");
        }
        final int nContigs = contigSequences.size();
        // 4 bytes for the initial contig count, a null byte at the end of each sequence, and all the sequence bytes
        final int capacity = 4+nContigs+contigSequences.stream().mapToInt(seq -> seq.length).sum();
        final ByteBuffer contigBuf = ByteBuffer.allocateDirect(capacity);
        contigBuf.order(ByteOrder.nativeOrder());
        contigBuf.putInt(nContigs);
        contigSequences.forEach(seq -> contigBuf.put(seq).put((byte)0));
        contigBuf.flip();
        ByteBuffer alignsBuf = createAlignments(contigBuf, indexAddress);
        if ( alignsBuf == null ) {
            throw new GATKException("Unable to get alignments from bwa-mem. We don't know why.");
        }
        alignsBuf.order(ByteOrder.nativeOrder()).position(0).limit(alignsBuf.capacity());
        final List<AlignmentRegion> alignRegions = new ArrayList<>(nContigs);
        final String assemblyName = "assembly"+assemblyId;
        for ( int contigId = 0; contigId != nContigs; ++contigId ) {
            String contigName = "tig"+(contigId+1);
            int nAligns = alignsBuf.getInt();
            while ( nAligns-- > 0 ) {
                final int refId = alignsBuf.getInt();
                final int refStartPos = alignsBuf.getInt() + 1;
                final int refEndPos = alignsBuf.getInt() + 1;
                final int tigStartPos = alignsBuf.getInt() + 1;
                final int tigEndPos = alignsBuf.getInt() + 1;
                final int mapQual = alignsBuf.getInt();
                final boolean isForwardStrand = refStartPos <= refEndPos;
                final String refContigName = refContigNames.get(refId);
                final SimpleInterval interval;
                if ( isForwardStrand ) interval = new SimpleInterval(refContigName, refStartPos, refEndPos);
                else interval = new SimpleInterval(refContigName, refEndPos, refStartPos);
                alignRegions.add(
                        new AlignmentRegion(assemblyName, contigName, new Cigar(), isForwardStrand, interval,
                                            mapQual, tigStartPos, tigEndPos, 0));
            }
        }
        destroyAlignments(alignsBuf);
        return alignRegions;
    }

    private static native long createIndex( String indexImageFile );
    private static native ByteBuffer getRefContigNames( long indexAddress );
    private static native void destroyRefContigNames( ByteBuffer refContigNames );
    private static native int destroyIndex( long indexAddress );
    private static native ByteBuffer createAlignments( ByteBuffer contigs, long indexAddress );
    private static native void destroyAlignments( ByteBuffer alignments );
}
