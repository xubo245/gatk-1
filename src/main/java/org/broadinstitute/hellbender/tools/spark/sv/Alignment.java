package org.broadinstitute.hellbender.tools.spark.sv;

public class Alignment {
    private final int refId; // index into reference dictionary
    private final int refStart; // 0-based coordinate, inclusive
    private final int refEnd; // 0-based coordinate, exclusive
    private final boolean isRC; // tig mapped to opposite strand on ref
    private final int tigStart; // 0-based coordinate, inclusive
    private final int tigEnd; // 0-based coordinate, exclusive
    private final boolean isAlternateMapping; // secondary alignment for this tig region
    private final int mapQual; // phred-scaled mapping quality

    public Alignment( final int refId, final int refStart, final int refEnd, final boolean isRC,
                      final int tigStart, final int tigEnd, final boolean isAlternateMapping, final int mapQual ) {
        this.refId = refId;
        this.refStart = refStart;
        this.refEnd = refEnd;
        this.isRC = isRC;
        this.tigStart = tigStart;
        this.tigEnd = tigEnd;
        this.isAlternateMapping = isAlternateMapping;
        this.mapQual = mapQual;
    }

    public int getRefId() { return refId; }
    public int getRefStart() { return refStart; }
    public int getRefEnd() { return refEnd; }
    public boolean isRC() { return isRC; }
    public int getTigStart() { return tigStart; }
    public int getTigEnd() { return tigEnd; }
    public boolean isAlternateMapping() { return isAlternateMapping; }
    public int getMapQual() { return mapQual; }
}
