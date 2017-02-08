package org.broadinstitute.hellbender.tools.spark.sv;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import htsjdk.samtools.Cigar;
import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;
import htsjdk.samtools.TextCigarCodec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.Utils;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.broadinstitute.hellbender.tools.spark.sv.ContigsCollection.loadContigsCollectionKeyedByAssemblyId;

/**
 * Implements a parser for parsing alignments of locally assembled contigs, and
 * generating {@link ChimericAlignment}'s from the alignments, if possible.
 */
class AssemblyAlignmentParser implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LogManager.getLogger(AssemblyAlignmentParser.class);
    private static final boolean DEBUG_STATS = false;

    /**
     * Loads the alignment regions and sequence of all locally-assembled contigs from the text file they are in;
     * one record for each contig.
     * @param ctx                       spark context for IO operations
     * @param pathToInputAlignments     path string to alignments of the contigs; format assumed to be consistent/parsable by {@link AlignmentRegion#toString()}
     * @return                          an PairRDD for all assembled contigs with their alignment regions and sequence
     */
    static JavaPairRDD<Iterable<AlignmentRegion>, byte[]> prepAlignmentRegionsForCalling(final JavaSparkContext ctx,
                                                                                         final String pathToInputAlignments,
                                                                                         final String pathToInputAssemblies) {

        final JavaPairRDD<Tuple2<String, String>, Iterable<AlignmentRegion>> alignmentRegionsKeyedByAssemblyAndContigId
                = parseAlignments(ctx, pathToInputAlignments);
        if (DEBUG_STATS) {
            AssemblyAlignmentParser.debugStats(alignmentRegionsKeyedByAssemblyAndContigId, pathToInputAlignments);
        }

        final JavaPairRDD<Tuple2<String, String>, byte[]> contigSequences
                = loadContigSequence(ctx, pathToInputAssemblies);

        return alignmentRegionsKeyedByAssemblyAndContigId
                .join(contigSequences)
                .mapToPair(Tuple2::_2);
    }

    // TODO: 11/23/16 test
    static JavaPairRDD<Tuple2<String, String>, Iterable<AlignmentRegion>> parseAlignments(final JavaSparkContext ctx,
                                                                                          final String pathToInputAlignments) {

        return ctx.textFile(pathToInputAlignments).map(textLine -> AlignmentRegion.fromString(textLine.split(AlignmentRegion.STRING_REP_SEPARATOR,-1)))
                .flatMap(oneRegion -> breakGappedAlignment(oneRegion, SVConstants.CallingStepConstants.GAPPED_ALIGNMENT_BREAK_DEFAULT_SENSITIVITY).iterator())
                .mapToPair(alignmentRegion -> new Tuple2<>(new Tuple2<>(alignmentRegion.assemblyId, alignmentRegion.contigId), alignmentRegion))
                .groupByKey();
    }

    // TODO: 12/15/16 test
    static JavaPairRDD<Tuple2<String, String>, byte[]> loadContigSequence(final JavaSparkContext ctx,
                                                                          final String pathToInputAssemblies) {
        return loadContigsCollectionKeyedByAssemblyId(ctx, pathToInputAssemblies)
                .flatMapToPair(assemblyIdAndContigsCollection -> {
                    final String assemblyId = assemblyIdAndContigsCollection._1;
                    final ContigsCollection contigsCollection = assemblyIdAndContigsCollection._2;
                    return contigsCollection.getContents().stream()
                            .map(pair -> new Tuple2<>(new Tuple2<>(assemblyId, pair._1.toString()), pair._2.toString().getBytes()))
                            .collect(Collectors.toList()).iterator();
                });
    }

    // TODO: 1/18/17 handle mapping quality and mismatches of the new alignment better rather than artificial ones. this would ultimately require a re-alignment done right.
    /**
     * Break a gapped alignment into multiple alignment regions, based on input sensitivity: i.e. if the gap(s) in the input alignment
     * is equal to or larger than the size specified by {@code sensitivity}, two alignment regions will be generated.
     *
     * To fulfill this functionality, needs to accomplish three key tasks correctly:
     * <ul>
     *     <li>generate appropriate cigar,</li>
     *     <li>infer reference coordinates,</li>
     *     <li>infer contig coordinates</li>
     * </ul>
     * As an example, an alignment with CIGAR "397S118M2D26M6I50M7I26M1I8M13D72M398S", when {@code sensitivity} is set to "1", should be broken into 5 alignments
     * <ul>
     *     <li>"397S118M594S"</li>
     *     <li>"515S26M568S"</li>
     *     <li>"547S50M512S"</li>
     *     <li>"631S8M470S"</li>
     *     <li>"639S72M398S"</li>
     * </ul>
     * On the other hand, an alignment with CIGAR "10M10D10M60I10M10I10M50D10M", when {@code sensitivity} is set to "50", should be broken into 3 alignments
     * <ul>
     *     <li>"10M10D10M100S"</li>
     *     <li>"80S10M10I10M10S"</li>
     *     <li>"110S10M"</li>
     * </ul>
     * And when an alignment has hard clipping adjacent to soft clippings, e.g. "1H2S3M5I10M20D6M7S8H", it should be broken into alignments with CIGAR resembling the original CIGAR as much as possible:
     * <ul>
     *     <li>"1H2S3M28S8H"</li>
     *     <li>"1H10S10M13S8H"</li>
     *     <li>"1H20S6M7S8H"</li>
     * </ul>
     *
     * @return an iterable of size >= 1. if size==1, the returned iterable contains only the input (i.e. either no gap or hasn't reached sensitivity)
     */
    @VisibleForTesting
    static Iterable<AlignmentRegion> breakGappedAlignment(final AlignmentRegion oneRegion, final int sensitivity) {

        final List<CigarElement> cigarElements = checkCigarAndConvertTerminalInsertionToSoftClip(oneRegion.cigarAlong5to3DirectionOfContig);
        if (cigarElements.size() == 1) return new ArrayList<>( Collections.singletonList(oneRegion) );

        final List<AlignmentRegion> result = new ArrayList<>(3); // blunt guess
        final int originalMapQ = oneRegion.mapQual;

        final List<CigarElement> cigarMemoryList = new ArrayList<>();
        final int clippedNBasesFromStart = SVVariantCallerUtils.getNumClippedBases(true, cigarElements);

        final int hardClippingAtBeginning = cigarElements.get(0).getOperator()==CigarOperator.H ? cigarElements.get(0).getLength() : 0;
        final int hardClippingAtEnd = (cigarElements.get(cigarElements.size()-1).getOperator()== CigarOperator.H)? cigarElements.get(cigarElements.size()-1).getLength() : 0;
        final CigarElement hardClippingAtBeginningMaybeNull = hardClippingAtBeginning==0 ? null : new CigarElement(hardClippingAtBeginning, CigarOperator.H);
        int contigIntervalStart = 1 + clippedNBasesFromStart;
        // we are walking along the contig following the cigar, which indicates that we might be walking backwards on the reference if oneRegion.forwardStrand==false
        int refBoundary1stInTheDirectionOfContig = oneRegion.forwardStrand ? oneRegion.referenceInterval.getStart() : oneRegion.referenceInterval.getEnd();
        for (final CigarElement cigarElement : cigarElements) {
            final CigarOperator op = cigarElement.getOperator();
            final int operatorLen = cigarElement.getLength();
            switch (op) {
                case M: case EQ: case X: case S: case H:
                    cigarMemoryList.add(cigarElement);
                    break;
                case I: case D:
                    if (operatorLen < sensitivity) {
                        cigarMemoryList.add(cigarElement);
                        break;
                    }

                    // collapse cigar memory list into a single cigar for ref & contig interval computation
                    final Cigar memoryCigar = new Cigar(cigarMemoryList);
                    final int effectiveReadLen = memoryCigar.getReadLength() + SVVariantCallerUtils.getTotalHardClipping(memoryCigar) - SVVariantCallerUtils.getNumClippedBases(true, memoryCigar);

                    // task 1: infer reference interval taking into account of strand
                    final SimpleInterval referenceInterval;
                    if (oneRegion.forwardStrand) {
                        referenceInterval = new SimpleInterval(oneRegion.referenceInterval.getContig(),
                                                               refBoundary1stInTheDirectionOfContig,
                                                          refBoundary1stInTheDirectionOfContig + (memoryCigar.getReferenceLength()-1));
                    } else {
                        referenceInterval = new SimpleInterval(oneRegion.referenceInterval.getContig(),
                                                         refBoundary1stInTheDirectionOfContig - (memoryCigar.getReferenceLength()-1), // step backward
                                                               refBoundary1stInTheDirectionOfContig);
                    }

                    // task 2: infer contig interval
                    final int contigIntervalEnd = contigIntervalStart + effectiveReadLen - 1;

                    // task 3: now add trailing cigar element and create the real cigar for the to-be-returned AR
                    cigarMemoryList.add(new CigarElement(oneRegion.assembledContigLength-contigIntervalEnd-hardClippingAtEnd, CigarOperator.S));
                    if (hardClippingAtEnd != 0) { // be faithful to hard clipping (as the accompanying bases have be hard-clipped away)
                        cigarMemoryList.add(new CigarElement(hardClippingAtEnd, CigarOperator.H));
                    }
                    final Cigar cigarForNewAlignmentRegion = new Cigar(cigarMemoryList);

                    final AlignmentRegion split = new AlignmentRegion(oneRegion.assemblyId, oneRegion.contigId, referenceInterval, cigarForNewAlignmentRegion, oneRegion.forwardStrand, originalMapQ, SVConstants.CallingStepConstants.ARTIFICIAL_MISMATCH, contigIntervalStart, contigIntervalEnd);

                    result.add(split);

                    // update cigar memory
                    cigarMemoryList.clear();
                    if (hardClippingAtBeginningMaybeNull != null) {
                        cigarMemoryList.add(hardClippingAtBeginningMaybeNull); // be faithful about hard clippings
                    }
                    cigarMemoryList.add(new CigarElement(contigIntervalEnd - hardClippingAtBeginning + (op.consumesReadBases() ? operatorLen : 0), CigarOperator.S));

                    // update pointers into reference and contig
                    final int refBoundaryAdvance = op.consumesReadBases() ? memoryCigar.getReferenceLength() : memoryCigar.getReferenceLength() + operatorLen;
                    refBoundary1stInTheDirectionOfContig += oneRegion.forwardStrand ? refBoundaryAdvance : -refBoundaryAdvance;
                    contigIntervalStart += op.consumesReadBases() ? effectiveReadLen + operatorLen : effectiveReadLen;

                    break;
                default:
                    throw new GATKException("Alignment have N or P in it: " + oneRegion.toString()); // TODO: 1/20/17 still not quite sure if this is quite right, it doesn't blow up on NA12878 WGS, but who knows what happens in the future
            }
        }

        if (result.isEmpty()) {
            return new ArrayList<>(Collections.singletonList(oneRegion));
        }

        final SimpleInterval lastReferenceInterval;
        if (oneRegion.forwardStrand) {
            lastReferenceInterval =  new SimpleInterval(oneRegion.referenceInterval.getContig(), refBoundary1stInTheDirectionOfContig, oneRegion.referenceInterval.getEnd());
        } else {
            lastReferenceInterval =  new SimpleInterval(oneRegion.referenceInterval.getContig(), oneRegion.referenceInterval.getStart(), refBoundary1stInTheDirectionOfContig);
        }

        final Cigar lastForwardStrandCigar = new Cigar(cigarMemoryList);
        int clippedNBasesFromEnd = SVVariantCallerUtils.getNumClippedBases(false, cigarElements);
        result.add(new AlignmentRegion(oneRegion.assemblyId, oneRegion.contigId, lastReferenceInterval, lastForwardStrandCigar,
                oneRegion.forwardStrand, originalMapQ, SVConstants.CallingStepConstants.ARTIFICIAL_MISMATCH,
                contigIntervalStart, oneRegion.assembledContigLength-clippedNBasesFromEnd));

        return result;
    }

    /**
     * Checks the input CIGAR for assumption that operator 'D' is not immediately adjacent to clipping operators.
     * Then convert the 'I' CigarElement, if it is at either end (terminal) of the input cigar, to a corresponding 'S' operator.
     * Note that we allow CIGAR of the format '10H10S10I10M', but disallows the format if after the conversion the cigar turns into a giant clip,
     * e.g. '10H10S10I10S10H' is not allowed (if allowed, it becomes a giant clip of '10H30S10H' which is non-sense).
     *
     * @return a pair of number of clipped (hard and soft, including the ones from the converted terminal 'I') bases at the front and back of the
     *         input {@code cigarAlongInput5to3Direction}.
     *
     * @throws IllegalArgumentException when the checks as described above fail.
     */
    @VisibleForTesting
    static List<CigarElement> checkCigarAndConvertTerminalInsertionToSoftClip(final Cigar cigarAlongInput5to3Direction) {

        final List<CigarElement> cigarElements = new ArrayList<>(cigarAlongInput5to3Direction.getCigarElements());
        if (cigarElements.size()==1) {
            return cigarElements;
        }

        SVVariantCallerUtils.checkCigarElements(cigarElements);

        final List<CigarElement> convertedList = convertInsToSoftClipFromOneEnd(cigarElements, true);
        final List<CigarElement> result = convertInsToSoftClipFromOneEnd(convertedList, false);
        Utils.validateArg(cigarElements.stream().anyMatch(ele -> ele.getOperator().isAlignment()),
                "No alignment between ref and ctg after 'I' -> 'S' conversion, this suggest new edge case CIGAR record: " + TextCigarCodec.encode(cigarAlongInput5to3Direction));
        return result;
    }

    /**
     * Actually convert terminal 'I' to 'S' and in case there's an 'S' comes before 'I', compactify the two neighboring 'S' operations into one.
     *
     * @return the converted and compactified list of cigar elements
     */
    @VisibleForTesting
    static List<CigarElement> convertInsToSoftClipFromOneEnd(final List<CigarElement> cigarElements,
                                                             final boolean fromStart) {
        final int numHardClippingBasesFromOneEnd = SVVariantCallerUtils.getNumHardClippingBases(fromStart, cigarElements);
        final int numSoftClippingBasesFromOneEnd = SVVariantCallerUtils.getNumSoftClippingBases(fromStart, cigarElements);

        final int indexForFirstMaybeInsertion;
        if (numHardClippingBasesFromOneEnd==0 && numSoftClippingBasesFromOneEnd==0) { // no clipping
            indexForFirstMaybeInsertion = fromStart ? 0 : cigarElements.size()-1;
        } else if (numHardClippingBasesFromOneEnd==0 || numSoftClippingBasesFromOneEnd==0) { // one clipping
            indexForFirstMaybeInsertion = fromStart ? 1 : cigarElements.size()-2;
        } else {
            indexForFirstMaybeInsertion = fromStart ? 2 : cigarElements.size()-3;
        }

        final CigarElement element = cigarElements.get(indexForFirstMaybeInsertion);
        if (element.getOperator() == CigarOperator.I) {

            cigarElements.set(indexForFirstMaybeInsertion, new CigarElement(element.getLength(), CigarOperator.S));
            if (numSoftClippingBasesFromOneEnd!=0) { // compactify with possible neighboring clipping
                final List<CigarElement> result = new ArrayList<>(cigarElements.size());
                if (fromStart) {
                    if (numHardClippingBasesFromOneEnd!=0) { // some hard clipping happened
                        result.add(0, cigarElements.get(0));
                    }
                    result.add(new CigarElement(element.getLength()+cigarElements.get(indexForFirstMaybeInsertion-1).getLength(), CigarOperator.S));
                    if (indexForFirstMaybeInsertion!=cigarElements.size()-1) { // copy the rest, if any
                        result.addAll(cigarElements.subList(indexForFirstMaybeInsertion+1, cigarElements.size()));
                    }
                } else {
                    if (indexForFirstMaybeInsertion!=0) { // copy the rest, if any
                        result.addAll(cigarElements.subList(0, indexForFirstMaybeInsertion));
                    }
                    result.add(new CigarElement(element.getLength()+cigarElements.get(indexForFirstMaybeInsertion+1).getLength(), CigarOperator.S));
                    if (numHardClippingBasesFromOneEnd!=0) { // some hard clipping happened
                        result.add(cigarElements.get(cigarElements.size() - 1));
                    }
                }
                return result;
            } else {
                return cigarElements;
            }
        } else {
            return cigarElements;
        }
    }

    private static void debugStats(final JavaPairRDD<Tuple2<String, String>, Iterable<AlignmentRegion>> alignmentRegionsWithContigSequences, final String outPrefix) {
        log.info(alignmentRegionsWithContigSequences.count() + " contigs");
        final long noARs = alignmentRegionsWithContigSequences.filter(tuple2 -> Iterables.size(tuple2._2())==0).count();
        log.info(noARs + " contigs have no alignments");
        final long oneARs = alignmentRegionsWithContigSequences.filter(tuple2 -> Iterables.size(tuple2._2())==1).count();
        log.info(oneARs + " contigs have only one alignments");

        final JavaPairRDD<String, List<Tuple2<Integer, Integer>>> x = alignmentRegionsWithContigSequences.filter(tuple2 -> Iterables.size(tuple2._2())==2).mapToPair(tuple2 -> {
            final Iterator<AlignmentRegion> it = tuple2._2().iterator();
            final AlignmentRegion region1 = it.next(), region2 = it.next();
            return new Tuple2<>(region1.assemblyId+":"+region1.contigId, Arrays.asList(new Tuple2<>(region1.mapQual, region1.referenceInterval.size()), new Tuple2<>(region2.mapQual, region2.referenceInterval.size())));
        });
        x.coalesce(1).saveAsTextFile(outPrefix+"_withTwoAlignments");
        log.info(x.count() + " contigs have two alignments");

        final JavaPairRDD<String, List<Tuple2<Integer, Integer>>> y = alignmentRegionsWithContigSequences.filter(tuple2 -> Iterables.size(tuple2._2())>2).mapToPair(tuple2 -> {
            final AlignmentRegion region1 = tuple2._2().iterator().next();
            return new Tuple2<>(region1.assemblyId+":"+region1.contigId, StreamSupport.stream(tuple2._2().spliterator(), false).map(ar -> new Tuple2<>(ar.mapQual, ar.referenceInterval.size())).collect(Collectors.toList()));
        });
        y.coalesce(1).saveAsTextFile(outPrefix+"_withMoreThanTwoAlignments");
        log.info(y.count() + " contigs have more than two alignments");
    }
}
