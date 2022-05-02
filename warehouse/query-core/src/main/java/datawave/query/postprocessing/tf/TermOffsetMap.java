package datawave.query.postprocessing.tf;

import datawave.query.jexl.functions.TermFrequencyList;
import org.javatuples.Triplet;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * This encapsulates term frequencies information that is used when evaluating content:phrase and content:within functions in a JEXL query. It is also used to
 * store phrase indexes that have been found for matching hits so that we can retrieve excerpts when requested.
 */
public class TermOffsetMap {
    
    /**
     * The term frequencies, with their corresponding fields.
     */
    private final Map<String,TermFrequencyList> termFrequencies = new HashMap<>();
    
    /**
     * The phrase indexes found for hits.
     */
    private final PhraseIndexes phraseIndexes = new PhraseIndexes();
    
    public TermOffsetMap() {}
    
    public TermOffsetMap(Map<String,TermFrequencyList> termFrequencies) {
        if (termFrequencies != null) {
            this.termFrequencies.putAll(termFrequencies);
        }
    }
    
    /**
     * Put the {@link TermFrequencyList} for the specified field
     * 
     * @param field
     *            the field
     * @param termFrequencyList
     *            the term frequency list
     */
    public void putTermFrequencyList(String field, TermFrequencyList termFrequencyList) {
        termFrequencies.put(field, termFrequencyList);
    }
    
    /**
     * Return the term frequency list for the specified field.
     * 
     * @param field
     *            the field
     * @return the term frequency list
     */
    public TermFrequencyList getTermFrequencyList(String field) {
        return termFrequencies.get(field);
    }
    
    /**
     * Add a new phrase index pair found for a hit for the specified field
     * 
     * @param field
     *            the field
     * @param eventId
     *            the event id (see @TermFrequencyList.getEventId(Key))
     * @param start
     *            the phrase starting index
     * @param end
     *            the phrase ending index
     */
    public void addPhraseIndexTriplet(String field, String eventId, int start, int end) {
        phraseIndexes.addIndexTriplet(field, eventId, start, end);
    }
    
    /**
     * Return all phrase indexes found for hits for the specified field.
     * 
     * @param field
     *            the field
     * @return the phrase indexes
     */
    public Collection<Triplet<String,Integer,Integer>> getPhraseIndexes(String field) {
        return phraseIndexes.getIndices(field);
    }
    
    /**
     * Return the underlying {@link PhraseIndexes} object
     * 
     * @return
     */
    public PhraseIndexes getPhraseIndexes() {
        return phraseIndexes;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TermOffsetMap that = (TermOffsetMap) o;
        return Objects.equals(termFrequencies, that.termFrequencies) && Objects.equals(phraseIndexes, that.phraseIndexes);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(termFrequencies, phraseIndexes);
    }
    
    @Override
    public String toString() {
        return new StringJoiner(", ", TermOffsetMap.class.getSimpleName() + "[", "]").add("termFrequencies=" + termFrequencies)
                        .add("phraseIndexes=" + phraseIndexes).toString();
    }
}
