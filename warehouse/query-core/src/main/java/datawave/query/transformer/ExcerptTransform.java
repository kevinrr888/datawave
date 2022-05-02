package datawave.query.transformer;

import datawave.query.Constants;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.ExcerptFields;
import datawave.query.iterator.logic.TermFrequencyExcerptIterator;
import datawave.query.postprocessing.tf.PhraseIndexes;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;
import org.javatuples.Triplet;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class ExcerptTransform extends DocumentTransform.DefaultDocumentTransform {
    
    private static final Logger log = Logger.getLogger(ExcerptTransform.class);
    
    public static final String PHRASE_INDEXES_ATTRIBUTE = "PHRASE_INDEXES_ATTRIBUTE";
    public static final String HIT_EXCERPT = "HIT_EXCERPT";
    
    private final Map<String,String> excerptIteratorOptions = new HashMap<>();
    private final TermFrequencyExcerptIterator excerptIterator;
    private final ExcerptFields excerptFields;
    private final IteratorEnvironment env;
    private final SortedKeyValueIterator<Key,Value> source;
    
    public ExcerptTransform(ExcerptFields excerptFields, IteratorEnvironment env, SortedKeyValueIterator<Key,Value> source) {
        this(excerptFields, env, source, new TermFrequencyExcerptIterator());
    }
    
    protected ExcerptTransform(ExcerptFields excerptFields, IteratorEnvironment env, SortedKeyValueIterator<Key,Value> source,
                    TermFrequencyExcerptIterator excerptIterator) {
        this.excerptFields = excerptFields;
        this.env = env;
        this.source = source;
        this.excerptIterator = excerptIterator;
    }
    
    @Nullable
    @Override
    public Entry<Key,Document> apply(@Nullable Entry<Key,Document> entry) {
        if (entry != null) {
            Document document = entry.getValue();
            // Do not bother adding excerpts to transient documents.
            if (document.isToKeep()) {
                PhraseIndexes phraseIndexes = getPhraseIndexes(document);
                if (phraseIndexes != null) {
                    if (log.isTraceEnabled()) {
                        log.trace("Fetching phrase excerpts " + excerptFields + " for document " + document.getMetadata());
                    }
                    Set<String> excerpts = getExcerpts(phraseIndexes);
                    addExcerptsToDocument(excerpts, document);
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("Phrase indexes were not added to document " + document.getMetadata() + ", skipping");
                    }
                }
            }
        }
        return entry;
    }
    
    /**
     * Retrieve the phrase indexes from the {@value #PHRASE_INDEXES_ATTRIBUTE} attribute in the document.
     * 
     * @param document
     *            the document
     * @return the phrase indexes
     */
    private PhraseIndexes getPhraseIndexes(Document document) {
        if (document.containsKey(PHRASE_INDEXES_ATTRIBUTE)) {
            Content content = (Content) document.get(PHRASE_INDEXES_ATTRIBUTE);
            return PhraseIndexes.from(content.getContent());
        } else {
            return null;
        }
    }
    
    /**
     * Add the excerpts to the document as part of {@value #HIT_EXCERPT}.
     * 
     * @param excerpts
     *            the excerpts to add
     * @param document
     *            the document
     */
    private void addExcerptsToDocument(Set<String> excerpts, Document document) {
        Attributes attributes = new Attributes(true);
        for (String excerpt : excerpts) {
            Content content = new Content(excerpt, document.getMetadata(), true);
            attributes.add(content);
        }
        document.put(HIT_EXCERPT, attributes);
    }
    
    /**
     * Get the excerpts.
     * 
     * @param phraseIndexes
     *            the pre-identified phrase offsets
     * @return the excerpts
     */
    private Set<String> getExcerpts(PhraseIndexes phraseIndexes) {
        phraseIndexes = getOffsetPhraseIndexes(phraseIndexes);
        if (phraseIndexes.isEmpty()) {
            return Collections.emptySet();
        }
        
        // Fetch the excerpts.
        Set<String> excerpts = new HashSet<>();
        for (String field : phraseIndexes.getFields()) {
            Collection<Triplet<String,Integer,Integer>> indexes = phraseIndexes.getIndices(field);
            for (Triplet<String,Integer,Integer> indexPair : indexes) {
                String eventId = indexPair.getValue0();
                int start = indexPair.getValue1();
                int end = indexPair.getValue2();
                if (log.isTraceEnabled()) {
                    log.trace("Fetching excerpt [" + start + "," + end + "] for field " + field + " for document " + eventId.replace('\u0000', '/'));
                }
                
                // Construct the required range for this document.
                int split = eventId.indexOf('\u0000');
                Key startKey = new Key(eventId.substring(0, split), eventId.substring(split + 1));
                Key endKey = startKey.followingKey(PartialKey.ROW_COLFAM);
                Range range = new Range(startKey, true, endKey, false);
                
                String excerpt = getExcerpt(field, start, end, range);
                // Only retain non-blank excerpts.
                if (excerpt != null && !excerpt.isEmpty()) {
                    excerpts.add(excerpt);
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("Failed to find excerpt [" + start + "," + end + "] for field " + field + "for document " + eventId.replace('\u0000', '/'));
                    }
                }
            }
        }
        return excerpts;
    }
    
    /**
     * Get the excerpt for the specified field.
     * 
     * @param field
     *            the field
     * @param start
     *            the start index of the excerpt
     * @param end
     *            the end index of the excerpt
     * @param range
     *            the range to use when seeking
     * @return the excerpt
     */
    private String getExcerpt(String field, int start, int end, Range range) {
        excerptIteratorOptions.put(TermFrequencyExcerptIterator.FIELD_NAME, field);
        excerptIteratorOptions.put(TermFrequencyExcerptIterator.START_OFFSET, String.valueOf(start));
        excerptIteratorOptions.put(TermFrequencyExcerptIterator.END_OFFSET, String.valueOf(end));
        try {
            excerptIterator.init(source, excerptIteratorOptions, env);
            excerptIterator.seek(range, Collections.emptyList(), false);
            if (excerptIterator.hasTop()) {
                Key topKey = excerptIterator.getTopKey();
                String[] parts = topKey.getColumnQualifier().toString().split(Constants.NULL);
                // The column qualifier is expected to be field\0phrase.
                if (parts.length == 2) {
                    return parts[1];
                } else {
                    log.warn(TermFrequencyExcerptIterator.class.getSimpleName() + " returned top key with incorrectly-formatted column qualifier in key: "
                                    + topKey + " when scanning for excerpt [" + start + "," + end + "] for field " + field + " within range " + range);
                    return null;
                }
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to scan for excerpt [" + start + "," + end + "] for field " + field + " within range " + range, e);
        }
    }
    
    /**
     * Returned a filtered {@link PhraseIndexes} that only contains the fields for which excerpts are desired, with the indexes offset by the specified excerpt
     * offset.
     * 
     * @param phraseIndexes
     *            the original phrase indexes
     * @return the filtered, offset phrase indexes
     */
    private PhraseIndexes getOffsetPhraseIndexes(PhraseIndexes phraseIndexes) {
        PhraseIndexes offsetPhraseIndexes = new PhraseIndexes();
        for (String field : excerptFields.getFields()) {
            // Filter out phrases that are not in desired fields.
            if (phraseIndexes.containsField(field)) {
                Collection<Triplet<String,Integer,Integer>> indexes = phraseIndexes.getIndices(field);
                int offset = excerptFields.getOffset(field);
                // Ensure the offset is modified to encompass the target excerpt range.
                for (Triplet<String,Integer,Integer> indexPair : indexes) {
                    String eventId = indexPair.getValue0();
                    int start = indexPair.getValue1() <= offset ? 0 : indexPair.getValue1() - offset;
                    int end = indexPair.getValue2() + offset + 1; // Add 1 here to offset the non-inclusive end of the range that will be used when scanning.
                    offsetPhraseIndexes.addIndexTriplet(field, eventId, start, end);
                }
            }
        }
        return offsetPhraseIndexes;
    }
    
    /**
     * Add phrase excerpts to the documents from the given iterator.
     * 
     * @param in
     *            the iterator source
     * @return an iterator that will supply the enriched documents
     */
    public Iterator<Entry<Key,Document>> getIterator(final Iterator<Entry<Key,Document>> in) {
        return new Iterator<Entry<Key,Document>>() {
            
            Entry<Key,Document> next;
            
            @Override
            public boolean hasNext() {
                if (in.hasNext()) {
                    next = ExcerptTransform.this.apply(in.next());
                }
                return next != null;
            }
            
            @Override
            public Entry<Key,Document> next() {
                return next;
            }
        };
    }
}
