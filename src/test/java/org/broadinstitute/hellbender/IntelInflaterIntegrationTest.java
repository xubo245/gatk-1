package org.broadinstitute.hellbender;

import com.intel.gkl.compression.IntelInflater;
import com.intel.gkl.compression.IntelInflaterFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.utils.NativeUtils;
import org.broadinstitute.hellbender.utils.RandomDNA;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * Test that it's possible to load libIntelInflater
 */
public class IntelInflaterIntegrationTest extends BaseTest {

    private final static Logger log = LogManager.getLogger(IntelInflaterIntegrationTest.class);
    private static final String INPUT_FILE = publicTestDir + "CEUTrio.HiSeq.WGS.b37.NA12878.20.21.bam";


    private boolean isIntelInflaterSupported() {
        return (NativeUtils.runningOnLinux() || NativeUtils.runningOnMac()) && !NativeUtils.runningOnPPCArchitecture();
    }

    @Test
    public void testIntelInflaterIsAvailable(){
        if ( ! NativeUtils.runningOnLinux()  && ! NativeUtils.runningOnMac()) {
            throw new SkipException("IntelInflater not available on this platform");
        }

        if ( NativeUtils.runningOnPPCArchitecture() ) {
            throw new SkipException("IntelInflater not available for this architecture");
        }

        Assert.assertTrue(new IntelInflater().load(null), "IntelInflater shared library was not loaded. " +
                "This could be due to a configuration error, or your system might not support it.");
    }

    @Test
    public void inflateInflateWithIntelInflater() {
        if (!isIntelInflaterSupported()) {
            throw new SkipException("IntelInflater not available on this platform");
        }

        // create buffers and random input
        final int LEN = 64 * 1024;
        final byte[] input = new RandomDNA().nextBases(LEN);
        final byte[] compressed = new byte[2 * LEN];
        final byte[] result = new byte[LEN];

        final IntelInflaterFactory intelInflaterFactory = new IntelInflaterFactory();

        for (int i = 0; i < 10; i++) {
            // create deflater with compression level i
            final Deflater deflater = new Deflater(i, true);

            // setup deflater
            deflater.reset();
            deflater.setInput(input);
            deflater.finish();

            // compress data
            int compressedBytes = 0;
            while (!deflater.finished()) {
                compressedBytes = deflater.deflate(compressed, 0, compressed.length);
            }
            deflater.end();

            // log results
            log.info("%d bytes compressed to %d bytes : %2.2f%% compression\n",
                    LEN, compressedBytes, 100.0 - 100.0 * compressedBytes / LEN);

            // decompress and check output == input
            Inflater inflater = intelInflaterFactory.makeInflater(true);
            try {
                inflater.setInput(compressed, 0, compressedBytes);
                inflater.inflate(result, 0, result.length);
                inflater.end();
            } catch (java.util.zip.DataFormatException e) {
                e.printStackTrace();
            }

            Assert.assertEquals(input, result);
        }
    }
}
