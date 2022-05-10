package datawave.query.function;

import datawave.query.Constants;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.ValueTuple;
import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.DefaultArithmetic;
import datawave.query.jexl.DelayedNonEventIndexContext;
import datawave.query.postprocessing.tf.PhraseIndexes;
import datawave.query.postprocessing.tf.TermOffsetMap;
import datawave.query.transformer.ExcerptTransform;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.jexl2.DatawaveJexlScript;
import org.apache.commons.jexl2.ExpressionImpl;
import org.apache.commons.jexl2.JexlArithmetic;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.log4j.Logger;

import com.google.common.base.Predicate;

import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.jexl.HitListArithmetic;
import datawave.query.jexl.DatawaveJexlEngine;
import datawave.query.util.Tuple3;

import java.util.Collections;
import java.util.Set;

public class JexlEvaluation implements Predicate<Tuple3<Key,Document,DatawaveJexlContext>> {
    
    private static final Logger log = Logger.getLogger(JexlEvaluation.class);
    
    public static final String HIT_TERM_FIELD = "HIT_TERM";
    public static final String EVAL_STATE_FIELD = "EVAL_STATE";
    
    public enum EVAL_STATE {
        FULL, PARTIAL
    }
    
    private String query;
    private JexlArithmetic arithmetic;
    private DatawaveJexlEngine engine;
    
    /**
     * Compiled and flattened jexl script
     */
    protected DatawaveJexlScript script;
    
    public JexlEvaluation(String query) {
        this(query, new DefaultArithmetic());
    }
    
    public JexlEvaluation(String query, JexlArithmetic arithmetic) {
        this(query, arithmetic, false, Collections.emptySet());
    }
    
    public JexlEvaluation(String query, JexlArithmetic arithmetic, boolean usePartialInterpreter, Set<String> incompleteFields) {
        this.query = query;
        this.arithmetic = arithmetic;
        
        // Get a JexlEngine initialized with the correct JexlArithmetic for this Document
        if (usePartialInterpreter) {
            this.engine = ArithmeticJexlEngines.getEngine(arithmetic, incompleteFields);
        } else {
            this.engine = ArithmeticJexlEngines.getEngine(arithmetic);
        }
        
        // Evaluate the JexlContext against the Script
        this.script = DatawaveJexlScript.create((ExpressionImpl) this.engine.createScript(this.query));
    }
    
    public JexlArithmetic getArithmetic() {
        return arithmetic;
    }
    
    public DatawaveJexlEngine getEngine() {
        return engine;
    }
    
    public ASTJexlScript parse(CharSequence expression) {
        return engine.parse(expression);
    }
    
    public boolean isMatched(Object o) {
        return isMatched(o, engine.getUsePartialInterpreter());
    }
    
    public boolean isMatched(Object o, boolean usePartialInterpreter) {
        return ArithmeticJexlEngines.isMatched(o, usePartialInterpreter);
    }
    
    @Override
    public boolean apply(Tuple3<Key,Document,DatawaveJexlContext> input) {
        
        Object o = script.execute(input.third());
        
        if (log.isTraceEnabled()) {
            log.trace("Evaluation of " + query + " against " + input.third() + " returned " + o);
        }
        
        boolean matched = isMatched(o);
        
        // Add delayed info to document
        if (matched && input.third() instanceof DelayedNonEventIndexContext) {
            ((DelayedNonEventIndexContext) input.third()).populateDocument(input.second());
        }
        
        // pass a hint back to the webservice about the evaluation state
        if (engine.wasCallbackUsed() && matched) {
            Document document = input.second();
            Content attr = new Content(String.valueOf(EVAL_STATE.PARTIAL), document.getMetadata(), document.isToKeep());
            document.put(EVAL_STATE_FIELD, attr);
            engine.resetCallback();
        }
        
        if (arithmetic instanceof HitListArithmetic) {
            HitListArithmetic hitListArithmetic = (HitListArithmetic) arithmetic;
            if (matched) {
                Document document = input.second();
                
                Attributes attributes = new Attributes(input.second().isToKeep());
                
                for (ValueTuple hitTuple : hitListArithmetic.getHitTuples()) {
                    
                    ColumnVisibility cv = null;
                    String term = hitTuple.getFieldName() + ':' + hitTuple.getValue();
                    
                    if (hitTuple.getSource() != null) {
                        cv = hitTuple.getSource().getColumnVisibility();
                    }
                    
                    // fall back to extracting column visibility from document
                    if (cv == null) {
                        // get the visibility for the record with this hit
                        cv = HitListArithmetic.getColumnVisibilityForHit(document, term);
                        // if no visibility computed, then there were no hits that match fields still in the document......
                    }
                    
                    if (cv != null) {
                        // unused
                        long timestamp = document.getTimestamp(); // will force an update to make the metadata valid
                        Content content = new Content(term, document.getMetadata(), document.isToKeep());
                        content.setColumnVisibility(cv);
                        attributes.add(content);
                        
                    }
                }
                if (attributes.size() > 0) {
                    document.put(HIT_TERM_FIELD, attributes);
                }
                
                // Put the phrase indexes into the document so that we can add phrase excerpts if desired later.
                TermOffsetMap termOffsetMap = (TermOffsetMap) input.third().get(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME);
                if (termOffsetMap != null) {
                    PhraseIndexes phraseIndexes = termOffsetMap.getPhraseIndexes();
                    Content phraseIndexesAttribute = new Content(phraseIndexes.toString(), document.getMetadata(), false);
                    document.put(ExcerptTransform.PHRASE_INDEXES_ATTRIBUTE, phraseIndexesAttribute);
                    if (log.isTraceEnabled()) {
                        log.trace("Added phrase-indexes " + phraseIndexes + " as attribute " + ExcerptTransform.PHRASE_INDEXES_ATTRIBUTE + " to document "
                                        + document.getMetadata());
                    }
                }
            }
            hitListArithmetic.clear();
        }
        return matched;
    }
    
}
