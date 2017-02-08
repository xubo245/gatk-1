package org.broadinstitute.hellbender.tools.spark.sv;

import htsjdk.variant.variantcontext.Allele;

import static org.broadinstitute.hellbender.tools.spark.sv.NovelAdjacencyReferenceLocations.EndConnectionType.FIVE_TO_FIVE;

/**
 * Created by shuang on 2/7/17.
 */
abstract class SVTYPE {

    enum RECOGNIZED_SVTYPES {
        INV, DEL, INS, DUP;
    }

    abstract String getVariantId();

    abstract Allele getAltAllele();

    abstract int getSVLength();


    static final class INV extends SVTYPE {
        private final String variantId;
        private final Allele altAllele;
        private final int svLen;

        String getVariantId() {
            return variantId;
        }
        Allele getAltAllele() {
            return altAllele;
        }
        int getSVLength() {
            return svLen;
        }
        @Override
        public String toString() {
            return RECOGNIZED_SVTYPES.INV.name();
        }

        INV(final NovelAdjacencyReferenceLocations novelAdjacencyReferenceLocations) {
            final String contig = novelAdjacencyReferenceLocations.leftJustifiedLeftRefLoc.getContig();
            final int start = novelAdjacencyReferenceLocations.leftJustifiedLeftRefLoc.getEnd();
            final int end = novelAdjacencyReferenceLocations.leftJustifiedRightRefLoc.getStart();
            final NovelAdjacencyReferenceLocations.EndConnectionType endConnectionType = novelAdjacencyReferenceLocations.endConnectionType;

            variantId = (endConnectionType == FIVE_TO_FIVE ? GATKSVVCFHeaderLines.INV55 : GATKSVVCFHeaderLines.INV33) + SVConstants.CallingStepConstants.VARIANT_ID_FIELD_SEPARATOR +
                    contig + SVConstants.CallingStepConstants.VARIANT_ID_FIELD_SEPARATOR + start + SVConstants.CallingStepConstants.VARIANT_ID_FIELD_SEPARATOR + end;

            altAllele = Allele.create(SVConstants.CallingStepConstants.VCF_ALT_ALLELE_STRING_INV);

            svLen = end - start;
        }
    }

    static final class DEL extends SVTYPE {
        private final String variantId;
        private final Allele altAllele;
        private final int svLen;

        String getVariantId() {
            return variantId;
        }
        Allele getAltAllele() {
            return altAllele;
        }
        int getSVLength() {
            return svLen;
        }
        @Override
        public String toString() {
            return RECOGNIZED_SVTYPES.DEL.name();
        }

        DEL(final NovelAdjacencyReferenceLocations novelAdjacencyReferenceLocations) {
            final String contig = novelAdjacencyReferenceLocations.leftJustifiedLeftRefLoc.getContig();
            final int start = novelAdjacencyReferenceLocations.leftJustifiedLeftRefLoc.getEnd();
            final int end = novelAdjacencyReferenceLocations.leftJustifiedRightRefLoc.getStart();

            final String startString;
            if (novelAdjacencyReferenceLocations.complication.dupSeqForwardStrandRep.isEmpty()) {
                startString = toString();
            } else {
                startString = "DUP" +
                        ((novelAdjacencyReferenceLocations.complication.dupSeqRepeatNumOnRef < novelAdjacencyReferenceLocations.complication.dupSeqRepeatNumOnCtg) ? SVConstants.CallingStepConstants.TANDUP_EXPANSION_STRING : SVConstants.CallingStepConstants.TANDUP_CONTRACTION_STRING);
            }
            variantId =  startString + SVConstants.CallingStepConstants.VARIANT_ID_FIELD_SEPARATOR +
                    contig + SVConstants.CallingStepConstants.VARIANT_ID_FIELD_SEPARATOR + start + SVConstants.CallingStepConstants.VARIANT_ID_FIELD_SEPARATOR + end;

            altAllele = Allele.create(SVConstants.CallingStepConstants.VCF_ALT_ALLELE_STRING_DEL);
            svLen = -(end - start);
        }
    }

    static final class INS extends SVTYPE {
        private final String variantId;
        private final Allele altAllele;
        private final int svLen;

        String getVariantId() {
            return variantId;
        }
        Allele getAltAllele() {
            return altAllele;
        }
        int getSVLength() {
            return svLen;
        }
        @Override
        public String toString() {
            return RECOGNIZED_SVTYPES.INS.name();
        }

        INS(final NovelAdjacencyReferenceLocations novelAdjacencyReferenceLocations) {
            final String contig = novelAdjacencyReferenceLocations.leftJustifiedLeftRefLoc.getContig();
            final int start = novelAdjacencyReferenceLocations.leftJustifiedLeftRefLoc.getEnd();
            final int end = novelAdjacencyReferenceLocations.leftJustifiedRightRefLoc.getStart();

            final String startString;
            if (novelAdjacencyReferenceLocations.complication.dupSeqForwardStrandRep.isEmpty()) {
                startString = toString();
            } else {
                startString = "DUP" +
                        ((novelAdjacencyReferenceLocations.complication.dupSeqRepeatNumOnRef < novelAdjacencyReferenceLocations.complication.dupSeqRepeatNumOnCtg) ? SVConstants.CallingStepConstants.TANDUP_EXPANSION_STRING : SVConstants.CallingStepConstants.TANDUP_CONTRACTION_STRING);
            }
            variantId =  startString + SVConstants.CallingStepConstants.VARIANT_ID_FIELD_SEPARATOR +
                    contig + SVConstants.CallingStepConstants.VARIANT_ID_FIELD_SEPARATOR + start + SVConstants.CallingStepConstants.VARIANT_ID_FIELD_SEPARATOR + end;

            altAllele = Allele.create(SVConstants.CallingStepConstants.VCF_ALT_ALLELE_STRING_INS);
            svLen = novelAdjacencyReferenceLocations.complication.insertedSequenceForwardStrandRep.length()
                    + (novelAdjacencyReferenceLocations.complication.dupSeqRepeatNumOnCtg - novelAdjacencyReferenceLocations.complication.dupSeqRepeatNumOnRef)*novelAdjacencyReferenceLocations.complication.dupSeqForwardStrandRep.length();
        }
    }

    static final class DUP extends SVTYPE {
        private final String variantId;
        private final Allele altAllele;
        private final int svLen;

        String getVariantId() {
            return variantId;
        }
        Allele getAltAllele() {
            return altAllele;
        }
        int getSVLength() {
            return svLen;
        }
        @Override
        public String toString() {
            return RECOGNIZED_SVTYPES.DUP.name();
        }

        DUP(final NovelAdjacencyReferenceLocations novelAdjacencyReferenceLocations) {
            final String contig = novelAdjacencyReferenceLocations.leftJustifiedLeftRefLoc.getContig();
            final int start = novelAdjacencyReferenceLocations.leftJustifiedLeftRefLoc.getEnd();
            final int end = novelAdjacencyReferenceLocations.leftJustifiedRightRefLoc.getStart();

            variantId = toString() +
                    ((novelAdjacencyReferenceLocations.complication.dupSeqRepeatNumOnRef < novelAdjacencyReferenceLocations.complication.dupSeqRepeatNumOnCtg) ? SVConstants.CallingStepConstants.TANDUP_EXPANSION_STRING : SVConstants.CallingStepConstants.TANDUP_CONTRACTION_STRING)
                    + SVConstants.CallingStepConstants.VARIANT_ID_FIELD_SEPARATOR +
                    contig + SVConstants.CallingStepConstants.VARIANT_ID_FIELD_SEPARATOR + start + SVConstants.CallingStepConstants.VARIANT_ID_FIELD_SEPARATOR + end;
            altAllele = Allele.create(SVConstants.CallingStepConstants.VCF_ALT_ALLELE_STRING_DUP);
            svLen = novelAdjacencyReferenceLocations.complication.insertedSequenceForwardStrandRep.length()
                    + (novelAdjacencyReferenceLocations.complication.dupSeqRepeatNumOnCtg - novelAdjacencyReferenceLocations.complication.dupSeqRepeatNumOnRef)*novelAdjacencyReferenceLocations.complication.dupSeqForwardStrandRep.length();
        }
    }
}
