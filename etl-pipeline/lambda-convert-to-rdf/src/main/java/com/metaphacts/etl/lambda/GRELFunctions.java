/*
 * Copyright (C) 2015-2023, metaphacts GmbH
 */
package com.metaphacts.etl.lambda;

import java.util.List;

import org.apache.commons.codec.EncoderException;

import io.carml.engine.function.FnoFunction;
import io.carml.engine.function.FnoParam;
import io.carml.engine.rdf.RdfRmlMapper.Builder;
import io.fno.grel.StringFunctions;

/**
 * GREL functions for RML processing.
 * 
 * <p>
 * This class provides wrappers for carml around the existing implementation.
 * </p>
 *
 * @see https://openrefine.org/docs/manual/grelfunctions#string-functions
 * @see https://users.ugent.be/~bjdmeest/function/grel.ttl
 * @see https://github.com/FnOio/grel-functions-java
 */
public class GRELFunctions {
    public final static String NAMESPACE_GREL = "http://users.ugent.be/~bjdmeest/function/grel.ttl#";

    /**
     * Register all GREL functions
     * 
     * @param mapperBuilder builder in which to register the functions
     */
    public static void register(Builder mapperBuilder) {
        mapperBuilder.addFunctions(new GRELStringFunctions());
    }

    public static class GRELStringFunctions {
        /**
         * @see StringFunctions#length(String)
         */
        @FnoFunction(NAMESPACE_GREL + "string_length")
        public Integer length(@FnoParam(NAMESPACE_GREL + "valueParam") String s) {
            if (s == null) {
                return null;
            }
            return StringFunctions.length(s);
        }
        
        /**
         * Takes any value type (string, number, date, boolean, error, null) and gives a string version of that value.
         * https://docs.openrefine.org/manual/grelfunctions#tostringo-string-format-optional
         */
        @FnoFunction(NAMESPACE_GREL + "string_toString")
        public String toString(@FnoParam(NAMESPACE_GREL + "param_any_e") String valueParameter) {
            if (valueParameter == null) {
                return null;
            }
            return StringFunctions.toString(valueParameter);
        }
        
        /**
         * Returns boolean indicating whether `s` starts with `sub`.
         * For example, `startsWith("food", "foo")` returns `true`, whereas `startsWith("food", "bar")` returns `false`.
         *
         * @param s   string
         * @param sub prefix
         * @return boolean
         */
        @FnoFunction(NAMESPACE_GREL + "string_startsWith")
        public Boolean startsWith(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "param_string_sub") String sub) {
            if (s == null) {
                return null;
            }
            return StringFunctions.startsWith(s, sub);
        }
        
        /**
         * Returns boolean indicating whether `s` ends with `sub`.
         * For example, `endsWith("food", "ood")` returns `true`, whereas `endsWith("food", "odd")` returns `false`.
         *
         * @param s   string
         * @param sub suffix
         * @return boolean
         */
        @FnoFunction(NAMESPACE_GREL + "string_endsWith")
        public Boolean endsWith(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "param_string_sub") String sub) {
            if (s == null) {
                return null;
            }
            return StringFunctions.endsWith(s, sub);
        }
        
        /**
         * Returns boolean indicating whether s ends with sub.
         * For example, endsWith("food", "ood") returns true, whereas endsWith("food", "odd") returns false.
         * You could also write the first case as "food".endsWith("ood").
         *
         * @param s
         * @param sub
         * @return
         */
        @FnoFunction(NAMESPACE_GREL + "string_contains")
        public Boolean contains(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "param_string_sub") String sub) {
            if (s == null) {
                return null;
            }
            return StringFunctions.contains(s, sub);
        }
        
        /**
         * Returns `s` converted to lowercase.
         *
         * @param s string
         * @return lowercase
         */
        @FnoFunction(NAMESPACE_GREL + "toLowerCase")
        public String toLowercase(@FnoParam(NAMESPACE_GREL + "valueParam") String s) {
            if (s == null) {
                return null;
            }
            return StringFunctions.toLowercase(s);
        }

        /**
         * Returns `s` converted to uppercase.
         *
         * @param s string
         * @return uppercase
         */
        @FnoFunction(NAMESPACE_GREL + "toUpperCase")
        public String toUppercase(@FnoParam(NAMESPACE_GREL + "valueParam") String s) {
            if (s == null) {
                return null;
            }
            return StringFunctions.toUppercase(s);
        }

        /**
         * https://docs.openrefine.org/manual/grelfunctions/#totitlecases
         * Returns string s converted into titlecase: a capital letter starting each word, and the rest of the letters lowercase.
         *
         * @param s
         * @return capitalized string
         */
        @FnoFunction(NAMESPACE_GREL + "string_toTitlecase")
        public String toTitlecase(@FnoParam(NAMESPACE_GREL + "valueParam") String s) {
            if (s == null) {
                return null;
            }
            return StringFunctions.toTitlecase(s);
        }

        /**
         * https://docs.openrefine.org/manual/grelfunctions#trims
         * Returns a copy of the string, with leading and trailing whitespace removed.
         * For example, `trim(" island ")` returns the string `island`.
         *
         * @param s string
         * @return a copy of the string, with leading and trailing whitespace removed
         */
        @FnoFunction(NAMESPACE_GREL + "string_trim")
        public String trim(@FnoParam(NAMESPACE_GREL + "valueParam") String s) {
            if (s == null) {
                return null;
            }
            return StringFunctions.trim(s);
        }

        /**
         * https://docs.openrefine.org/manual/grelfunctions#strips
         * Returns a copy of the string s with leading and trailing whitespace removed.
         * For example, " island ".strip() returns the string “island”. Identical to trim().
         * @param s string
         * @return a copy of the string s with leading and trailing whitespace removed
         */
        @FnoFunction(NAMESPACE_GREL + "string_strip")
        public String strip(@FnoParam(NAMESPACE_GREL + "valueParam") String s) {
            if (s == null) {
                return null;
            }
            return StringFunctions.strip(s);
        }

        /**
         * Returns a copy of s with sep removed from the end if s ends with sep; otherwise, just returns s.
         * For example, chomp("hardly", "ly") and chomp("hard", "ly") both return the string hard.
         * https://github.com/OpenRefine/OpenRefine/wiki/GREL-String-Functions#chompstring-s-string-sep
         *
         * @param s   string
         * @param sep sep
         * @return a copy of s with sep removed from the end if s ends with sep; otherwise, just returns s
         */
        @FnoFunction(NAMESPACE_GREL + "string_lastIndexOf")
        public String chomp(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "param_string_sub") String sep) {
            if (s == null) {
                return null;
            }
            return StringFunctions.chomp(s, sep);
        }

        /**
         * Returns the substring of `s` starting from character index `from` upto the end of the string `s`.
         * For example, `substring("profound", 3)` returns the string `found`.
         * <p>
         * Character indexes start from zero.
         *
         * @param s    string
         * @param from character index from
         * @return substring
         */
        @FnoFunction(NAMESPACE_GREL + "string_substring")
        public String substring(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "p_int_i_from") Integer from) {
            if (s == null) {
                return null;
            }
            return StringFunctions.substring(s, from);
        }

        /**
         * Returns the substring of `s` starting from character index `from` and upto character index `to`.
         * For example, `substring("profound", 2, 4)` returns the string `of`.
         * <p>
         * Character indexes start from zero.
         * Negative character indexes are understood as counting from the end of the string.
         * For example, `substring("profound", 1, -1)` returns the string `rofoun`.
         *
         * @param s    string
         * @param from character index from
         * @param to   character index upto
         * @return substring
         */
        @FnoFunction(NAMESPACE_GREL + "string_substring")
        public String substring(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "p_int_i_from") Integer from,
                @FnoParam(NAMESPACE_GREL + "p_int_i_opt_to") Integer to) {
            if (s == null) {
                return null;
            }
            return StringFunctions.substring(s, from, to);
        }

        /**
         * https://docs.openrefine.org/manual/grelfunctions#slices-n-from-n-to-optional
         * Identical to substring() in relation to strings.
         * @param s    string
         * @param from character index from
         * @param to   character index upto
         * @return substring
         */
        @FnoFunction(NAMESPACE_GREL + "string_slice")
        public String slice(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "p_int_i_from") Integer from,
                @FnoParam(NAMESPACE_GREL + "p_int_i_opt_to") Integer to) {
            if (s == null) {
                return null;
            }
            return StringFunctions.slice(s, from, to);
        }

        /**
         * https://docs.openrefine.org/manual/grelfunctions#slices-n-from-n-to-optional
         * Identical to substring() in relation to strings.
         * @param s    string
         * @param from character index from
         * @return substring
         */
        @FnoFunction(NAMESPACE_GREL + "string_slice")
        public String slice(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "p_int_i_from") Integer from) {
            if (s == null) {
                return null;
            }
            return StringFunctions.slice(s, from);
        }

        /**
         * https://docs.openrefine.org/manual/grelfunctions#slices-n-from-n-to-optional
         * Identical to substring() in relation to strings.
         * @param s    string
         * @param from character index from
         * @param to   character index upto
         * @return substring
         */
        @FnoFunction(NAMESPACE_GREL + "string_get")
        public String get(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "p_int_i_from") Integer from,
                @FnoParam(NAMESPACE_GREL + "p_int_i_opt_to") Integer to) {
            if (s == null) {
                return null;
            }
            return StringFunctions.get(s, from, to);
        }

        /**
         * https://docs.openrefine.org/manual/grelfunctions#slices-n-from-n-to-optional
         * Identical to substring() in relation to strings.
         * @param s    string
         * @param from character index from
         * @return substring
         */
        @FnoFunction(NAMESPACE_GREL + "string_get")
        public String get(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "p_int_i_from") Integer from) {
            if (s == null) {
                return null;
            }
            return StringFunctions.get(s, from, from);
        }

        /**
         * https://docs.openrefine.org/manual/grelfunctions#indexofs-sub
         * Returns the first character index of sub as it first occurs in s; or, returns -1 if s does not contain sub.
         * @param s
         * @param sub
         * @return character index
         */
        @FnoFunction(NAMESPACE_GREL + "string_indexOf")
        public Integer indexOf(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "param_string_sub") String sub) {
            if (s == null) {
                return null;
            }
            return StringFunctions.indexOf(s, sub);
        }

        /**
         * https://docs.openrefine.org/manual/grelfunctions/#lastindexofs-sub
         * Returns the first character index of sub as it last occurs in s; or, returns -1 if s does not contain sub.
         *
         * @param s
         * @param sub
         * @return character index
         */
        @FnoFunction(NAMESPACE_GREL + "string_lastIndexOf")
        public Integer lastIndexOf(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "param_string_sub") String sub) {
            if (s == null) {
                return null;
            }
            return StringFunctions.lastIndexOf(s, sub);
        }

        /**
         * https://docs.openrefine.org/manual/grelfunctions#replaces-s-or-p-find-s-replace
         * Returns the string obtained by replacing the find string with the replace string
         * in the inputted string.
         * @param s string to replace in
         * @param f target substring to replace
         * @param r string to replace target substring with
         * @return s with substring f replaced by string r
         */
        @FnoFunction(NAMESPACE_GREL + "string_replace")
        public String replace(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "param_find") String f,
                @FnoParam(NAMESPACE_GREL + "param_replace") String r) {
            if (s == null) {
                return null;
            }
            return StringFunctions.replace(s, f, r);
        }

        /**
         * https://docs.openrefine.org/manual/grelfunctions#replacecharss-s-find-s-replace
         * Returns the string obtained by replacing a character in s, identified by find,
         * with the corresponding character identified in replace.
         * You cannot use this to replace a single character with more than one character.
         *
         * For example, replaceChars("Téxt thát was optícálly recógnízéd", "áéíóú", "aeiou")
         * returns the string “Text that was optically recognized”.
         * @param s The string to search and replace characters in
         * @param f A string containing all the chars to replace
         * @param r A string containing all the chars to replace with. The ordering should be
         *          matched with the ordering in argument f
         * @throws Exception when the string of replacement chars is shorter than the string of
         *         characters to replace.
         */
        @FnoFunction(NAMESPACE_GREL + "string_replaceChars")
        public String replaceChars(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "param_find") String f, @FnoParam(NAMESPACE_GREL + "param_replace") String r)
                throws Exception {
            if (s == null) {
                return null;
            }
            return StringFunctions.replaceChars(s, f, r);
        }

        /**
         * https://docs.openrefine.org/manual/grelfunctions#matchs-p
         * Attempts to match the string s in its entirety against the regex pattern p and,
         * if the pattern is found, outputs an array of all capturing groups (found in order).
         * @param s string
         * @param p regex pattern
         * @return Array of pattern matches
         */
        @FnoFunction(NAMESPACE_GREL + "string_match")
        public String[] match(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "param_regex") String p) {
            if (s == null) {
                return null;
            }
            return StringFunctions.match(s, p);
        }

        /**
         * Returns a string converted to a number. Will attempt to convert other formats into a string,
         * then into a number. If the value is already a number, it will return the number.
         * https://docs.openrefine.org/manual/grelfunctions#tonumbers
         */
        @FnoFunction(NAMESPACE_GREL + "string_toNumber")
        public Integer toNumber(@FnoParam(NAMESPACE_GREL + "param_any_e") String o) {
            if (o == null) {
                return null;
            }
            return StringFunctions.toNumber(o);
        }

        /**
         * Returns the array of strings obtained by splitting `s` at wherever `sep` is found in it.
         * `sep` can be either a string or a regular expression.
         * For example, `split("fire, water, earth, air", ",")` returns the array of 4 strings:
         * "fire", " water", " earth" , and " air".
         * The double quotation marks are shown here only to highlight the fact that the spaces are retained.
         *
         * @param s   string
         * @param sep separator
         * @return the array of strings obtained by splitting `s` at wherever `sep` is found in it
         */
        @FnoFunction(NAMESPACE_GREL + "string_split")
        public List<String> split(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "param_string_sep") String sep) {
            if (s == null) {
                return null;
            }
            return StringFunctions.split(s, sep);
        }

        /**
         * https://docs.openrefine.org/manual/grelfunctions#splitbylengthss-n1-n2-
         * Returns the array of strings obtained by splitting s into substrings with the given
         * lengths. For example, "internationalization".splitByLengths(5, 6, 3) returns an array
         * of 3 strings: [ "inter", "nation", "ali" ]. Excess characters are discarded.
         * @param s string
         * @param numbers lengths of subsequent substrings to be extracted
         * @return Array of strings after splitting
         */
        @FnoFunction(NAMESPACE_GREL + "string_splitByLengths")
        public String[] splitByLengths(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "param_int_i") int number1,
                @FnoParam(NAMESPACE_GREL + "param_int_i2") int number2,
                @FnoParam(NAMESPACE_GREL + "param_int_rep_i") int number3) {
            if (s == null) {
                return null;
            }
            return StringFunctions.splitByLengths(s, new int[] { number1, number2, number3 });
        }

        /**
         * https://docs.openrefine.org/manual/grelfunctions#smartsplits-s-or-p-sep-optional
         * Returns the array of strings obtained by splitting s by the separator sep. Handles quotes properly.
         * Guesses tab or comma separator if sep is not given.
         * Also, value.escape('javascript') is useful for previewing unprintable chars prior to using smartSplit.
         */
        @FnoFunction(NAMESPACE_GREL + "string_smartSplit")
        public String[] smartSplit(@FnoParam(NAMESPACE_GREL + "valueParam") String s) {
            if (s == null) {
                return null;
            }
            return StringFunctions.smartSplit(s);
        }

        @FnoFunction(NAMESPACE_GREL + "string_smartSplit")
        public String[] smartSplit(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "param_opt_sep") String sep) {
            if (s == null) {
                return null;
            }
            return StringFunctions.smartSplit(s, sep);
        }

        /**
         * https://docs.openrefine.org/manual/grelfunctions#splitbychartypes
         * Returns an array of strings obtained by splitting s into groups of consecutive characters
         * each time the characters change Unicode categories. For example, "HenryCTaylor".splitByCharType()
         * will result in an array of [ "H", "enry", "CT", "aylor" ]. It is useful for separating letters
         * and numbers: "BE1A3E".splitByCharType() will result in [ "BE", "1", "A", "3", "E" ].
         */
        @FnoFunction(NAMESPACE_GREL + "string_splitByCharType")
        public String[] splitByCharType(@FnoParam(NAMESPACE_GREL + "valueParam") String value) {
            if (value == null) {
                return null;
            }
            return StringFunctions.splitByCharType(value);
        }

        /**
         * https://docs.openrefine.org/manual/grelfunctions#partitions-s-or-p-fragment-b-omitfragment-optional
         * Returns an array of strings [ a, fragment, z ] where a is the substring within s before the
         * first occurrence of fragment, and z is the substring after fragment. Fragment can be a
         * string or a regex.
         *
         * For example, "internationalization".partition("nation") returns 3 strings:
         * [ "inter", "nation", "alization" ]. If s does not contain fragment, it returns an array of
         * [ s, "", "" ] (the original unpartitioned string, and two empty strings).
         */
        @FnoFunction(NAMESPACE_GREL + "string_partition")
        public String[] partition(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "param_fragment") String frag) {
            if (s == null) {
                return null;
            }
            return StringFunctions.partition(s, frag);
        }

        /**
         * https://docs.openrefine.org/manual/grelfunctions#partitions-s-or-p-fragment-b-omitfragment-optional
         * Returns an array of strings [ a, fragment, z ] where a is the substring within s before the
         * first occurrence of fragment, and z is the substring after fragment. Fragment can be a
         * string or a regex.
         *
         * For example, "internationalization".partition("nation") returns 3 strings:
         * [ "inter", "nation", "alization" ]. If s does not contain fragment, it returns an array of
         * [ s, "", "" ] (the original unpartitioned string, and two empty strings).
         *
         * If the omitFragment boolean is true, for example with "internationalization".partition("nation", true),
         * the fragment is not returned. The output is [ "inter", "alization" ].
         */
        @FnoFunction(NAMESPACE_GREL + "string_partition")
        public String[] partition(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "param_fragment") String frag,
                @FnoParam(NAMESPACE_GREL + "param_bool_opt_b") Boolean omitFragment) {
            if (s == null) {
                return null;
            }
            return StringFunctions.partition(s, frag, omitFragment);
        }

        /**
         * https://docs.openrefine.org/manual/grelfunctions#rpartitions-s-or-p-fragment-b-omitfragment-optional
         * Returns an array of strings [ a, fragment, z ] where a is the substring within s before
         * the last occurrence of fragment, and z is the substring after the last instance of fragment.
         * (Rpartition means “reverse partition.”)
         *
         * For example, "parallel".rpartition("a") returns 3 strings:
         * [ "par", "a", "llel" ]. Otherwise works identically to partition().
         */
        @FnoFunction(NAMESPACE_GREL + "string_rpartition")
        public String[] rpartition(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "param_fragment") String frag) {
            if (s == null) {
                return null;
            }
            return StringFunctions.rpartition(s, frag);
        }

        /**
         * https://docs.openrefine.org/manual/grelfunctions#rpartitions-s-or-p-fragment-b-omitfragment-optional
         * Returns an array of strings [ a, fragment, z ] where a is the substring within s before
         * the last occurrence of fragment, and z is the substring after the last instance of fragment.
         * (Rpartition means “reverse partition.”)
         *
         * For example, "parallel".rpartition("a") returns 3 strings:
         * [ "par", "a", "llel" ]. Otherwise works identically to partition().
         */
        @FnoFunction(NAMESPACE_GREL + "string_rpartition")
        public String[] rpartition(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "param_fragment") String frag,
                @FnoParam(NAMESPACE_GREL + "param_bool_opt_b") Boolean omitFragment) {
            if (s == null) {
                return null;
            }
            return StringFunctions.rpartition(s, frag, omitFragment);
        }


        /**
         * https://docs.openrefine.org/manual/grelfunctions#diffs1-s2-s-timeunit-optional
         * Takes two strings and compares them, returning a string. Returns the remainder
         * of s2 starting with the first character where they differ.
         *
         * For example, diff("cacti", "cactus") returns "us".
         */
        @FnoFunction(NAMESPACE_GREL + "string_diff")
        public String diff(@FnoParam(NAMESPACE_GREL + "valueParam") String o1,
                @FnoParam(NAMESPACE_GREL + "valueParam2") String o2) {
            // diff can handle null values
            return StringFunctions.diff(o1, o2);
        }

        /**
         * Escapes `s` in the given escaping mode: `html`, `xml`, `csv`, `url`, `javascript`.
         *
         * @param s    string
         * @param mode mode
         * @return escaped
         */
        @FnoFunction(NAMESPACE_GREL + "escape")
        public String escape(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "modeParam") String mode) {
            if (s == null) {
                return null;
            }
            return StringFunctions.escape(s, mode);
        }

        /**
         * https://docs.openrefine.org/manual/grelfunctions#unescapes-s-mode
         * Unescapes s in the given escaping mode. The mode can be one of: "html", "xml",
         * "csv", "url", "javascript". Note that quotes are required around your mode.
         */
        @FnoFunction(NAMESPACE_GREL + "string_unescape")
        public String unescape(@FnoParam(NAMESPACE_GREL + "valueParam") String valueParameter,
                @FnoParam(NAMESPACE_GREL + "modeParam") String modeParameter) {
            if (valueParameter == null) {
                return null;
            }
            return StringFunctions.unescape(valueParameter, modeParameter);
        }

        /**
         * Returns the MD5 hash of an object. If fed something other than a string (array, number, date, etc.), md5() will convert it to a string and deliver the hash of the string.
         *
         * @param s
         * @return
         */
        @FnoFunction(NAMESPACE_GREL + "string_md5")
        public String md5(@FnoParam(NAMESPACE_GREL + "valueParam") String s) {
            if (s == null) {
                return null;
            }
            return StringFunctions.md5(s);
        }

        /**
         * Returns the SHA-1 hash of an object. If fed something other than a string (array, number, date, etc.), sha1() will convert it to a string and deliver the hash of the string.
         *
         * @param s
         * @return
         */
        @FnoFunction(NAMESPACE_GREL + "string_sha1")
        public String sha1(@FnoParam(NAMESPACE_GREL + "valueParam") String s) {
            if (s == null) {
                return null;
            }
            return StringFunctions.sha1(s);
        }

        /**
         * https://docs.openrefine.org/manual/grelfunctions#phonetics-s-encoding
         * Returns a phonetic encoding of a string, based on an available phonetic algorithm.
         * Can be one of the following supported phonetic methods: metaphone, doublemetaphone, metaphone3, soundex, cologne.
         *
         * @param s string to encode
         * @param mode "doublemetaphone", "metaphone", "metaphone3", "soundex", or "cologne"
         * @return encoded string
         * @throws EncoderException
         */
        @FnoFunction(NAMESPACE_GREL + "string_phonetic")
        public String phonetic(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "param_string_encoding") String mode) throws EncoderException {
            if (s == null) {
                return null;
            }
            return StringFunctions.phonetic(s, mode);
        }

        /**
         * https://docs.openrefine.org/manual/grelfunctions#reinterprets-s-encodertarget-s-encodersource
         */
        @FnoFunction(NAMESPACE_GREL + "string_reinterpret")
        public String reinterpret(@FnoParam(NAMESPACE_GREL + "valueParam") String s,
                @FnoParam(NAMESPACE_GREL + "param_string_encoder") String encoder) {
            if (s == null) {
                return null;
            }
            return StringFunctions.reinterpret(s, encoder);
        }

        // https://docs.openrefine.org/manual/grelfunctions#unicodes
        @FnoFunction(NAMESPACE_GREL + "string_unicode")
        public String[] unicode(@FnoParam(NAMESPACE_GREL + "valueParam") String s) {
            if (s == null) {
                return null;
            }
            return StringFunctions.unicode(s);
        }
    }
}
