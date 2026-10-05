/*
 * Writes the SBE golden fixtures in this directory.
 *
 * The fixtures are bytes the *reference* produced, and `golden.tsv` is the
 * *reference's* reading of them. That is the whole value of the set: both sides
 * of the comparison come from the same program the reference's own Java codecs
 * come from, so a disagreement with `deepmsg-codec` is this repository's defect
 * and never the tool's opinion. The classpath is therefore `aeron-all-1.53.2.jar`
 * and nothing else — the codecs are in it (117 classes under
 * `io/aeron/archive/codecs/`), the `MutableDirectBuffer` is agrona's from inside
 * the same jar, and the version is checked below rather than assumed.
 *
 * Two things make this one program rather than 49 hand-written ones. The value
 * table is a *rule*, applied to every field of every message, and the encoders
 * and decoders are driven by reflection over the schema. So a schema that gains
 * a field gains a fixture value for it with no edit here, and a field this
 * program cannot place stops the run rather than being quietly left at zero.
 *
 * The rule, exactly:
 *
 *   - integers and their aliases (`time_t`, `version_t`) get
 *     `lo + fnv1a(fieldName) % min(100, hi - lo + 1)`, where `lo` and `hi` come
 *     from the schema's `minValue`/`maxValue` where it declares them. The value
 *     depends on the field *name*, so the same field carries the same number in
 *     every message and a field landing in the wrong slot is a mismatch rather
 *     than a coincidence.
 *   - enums take the message's ordinal into the list of valid values. That
 *     reaches every variant of an enum that more than one message uses, because
 *     different messages land on different entries. An enum only one message
 *     uses — `RecordingSignal` has eight variants and one message — would sit
 *     on a single variant forever, so such a message gets one fixture per
 *     variant instead, named after it. `VARIANTS` overrides both for the one
 *     message that needs more than a rule.
 *   - variable-length fields carry their own name — `channel` is `"channel"` —
 *     which is deterministic, readable in the transcript, and varies in length
 *     without being asked to.
 *
 * No timestamp, no random number, no address, no iteration over a hash
 * container: two runs are byte-identical, which `git status` after a re-run
 * shows for free.
 *
 * Usage, from the repository root — `README.md` says where the jar comes from:
 *
 *   java --add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
 *        -cp ../aeron/aeron-all/build/libs/aeron-all-1.53.2.jar \
 *        tests/fixtures/sbe/Generate.java
 *
 * The `--add-exports` is agrona's, not ours: it reaches for
 * `jdk.internal.misc.Unsafe`, which the module system stopped handing out in
 * Java 17.
 */

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Stream;
import javax.xml.parsers.DocumentBuilderFactory;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public final class Generate {
    /** The generator, as a name. The golden is worth nothing if this drifts. */
    private static final String EXPECTED_JAR = "aeron-all-1.53.2.jar";

    /** The schemas to make fixtures for. Ids and versions are read from the files. */
    private static final List<String> SCHEMAS = List.of(
        "aeron-archive-codecs.xml",
        "aeron-archive-mark-codecs.xml");

    private static final int MESSAGE_HEADER_LENGTH = 8;

    /** The header's `version` field: the last two bytes of the message header, little-endian. */
    private static final int HEADER_VERSION_OFFSET = 6;

    /**
     * The messages whose fixtures the rules above do not describe.
     *
     * `ControlResponse` is the only one. It carries `ControlResponseCode`,
     * which no other message uses, so it would get a fixture per variant
     * anyway; what the rules cannot say is that `errorMessage` "will be empty
     * if code is OK" and that `version` is optional. The two ends of both are
     * stated here rather than left to a modulo.
     */
    private static final Map<String, List<Variant>> VARIANTS = Map.of(
        "ControlResponse", List.of(
            new Variant("error", Map.of("code", "ERROR")),
            new Variant("ok", Map.of("code", "OK", "errorMessage", "", "version", "0")),
            new Variant("recording-unknown", Map.of("code", "RECORDING_UNKNOWN")),
            new Variant("subscription-unknown", Map.of("code", "SUBSCRIPTION_UNKNOWN"))));

    public static void main(String[] args) {
        try {
            Path root = Paths.get("").toAbsolutePath();
            Path schemas = root.resolve("schemas");
            Path out = root.resolve("tests/fixtures/sbe");

            if (!Files.isDirectory(schemas)) {
                throw new IllegalStateException(
                    "run this from the repository root: " + schemas + " is not a directory");
            }
            checkClasspath();

            List<Fixture> fixtures = new ArrayList<>();
            for (String schemaFile : SCHEMAS) {
                fixtures.addAll(Schema.parse(schemas.resolve(schemaFile)).fixtures());
            }

            // Before anything is written, not after: a run that refuses
            // half-way leaves a directory of new `.bin` files beside a stale
            // `golden.tsv`, and the diff that makes is harder to read than no
            // diff at all.
            refuseStrays(out, fixtures);

            List<Reading> readings = new ArrayList<>();
            for (Fixture fixture : fixtures) {
                readings.addAll(fixture.run(out));
            }

            write(out, fixtures, readings);
            System.out.println("wrote " + fixtures.size() + " fixtures and " + readings.size()
                + " readings to " + out);
        } catch (Throwable t) {
            System.err.println("Generate: " + t);
            System.exit(1);
        }
    }

    /**
     * The codecs must come from the jar the fixtures were made with.
     *
     * A different build of the same classes is a different encoder, and the
     * failure it would cause is a golden mismatch blamed on `deepmsg-codec`.
     * Cheaper to refuse now; the version is already in the jar's own name.
     */
    private static void checkClasspath() {
        String location;
        try {
            location = io.aeron.archive.codecs.MessageHeaderDecoder.class
                .getProtectionDomain().getCodeSource().getLocation().getPath();
        } catch (Throwable t) {
            throw new IllegalStateException("the reference codecs are not on the classpath; see README.md", t);
        }
        if (!location.endsWith("/" + EXPECTED_JAR)) {
            throw new IllegalStateException("the codecs came from " + location + ", not " + EXPECTED_JAR);
        }
        System.out.println("classpath  " + location);
    }

    /** A named shape of a message, and the field values that shape pins. */
    record Variant(String suffix, Map<String, String> overrides) {
        String fixtureName(String messageKebab) {
            return suffix.isEmpty() ? messageKebab : messageKebab + "-" + suffix;
        }
    }

    // ---------------------------------------------------------------- model

    /** One `<sbe:message>`, fields in the order the encoder writes them. */
    record Message(int id, String name, String kebab, List<Field> fields) { }

    record Schema(int id, int version, String packageName, List<Message> messages) {
        static Schema parse(Path file) throws Exception {
            var factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            Element root = factory.newDocumentBuilder().parse(file.toFile()).getDocumentElement();

            Map<String, TypeSpec> types = parseTypes(root, root.getAttribute("package"));
            List<Message> messages = new ArrayList<>();
            for (Element element : children(root, "message")) {
                messages.add(parseMessage(element, types));
            }

            return new Schema(
                Integer.parseInt(root.getAttribute("id")),
                Integer.parseInt(root.getAttribute("version")),
                root.getAttribute("package"),
                List.copyOf(messages));
        }

        List<Fixture> fixtures() {
            List<Fixture> fixtures = new ArrayList<>();
            for (int ordinal = 0; ordinal < messages.size(); ordinal++) {
                Message message = messages.get(ordinal);
                for (Variant variant : variantsFor(message)) {
                    fixtures.add(new Fixture(this, message, ordinal, variant));
                }
            }
            return fixtures;
        }

        /**
         * One fixture for most messages, several for a few.
         *
         * The ordinal in `valueOf` walks an enum's valid values as the message
         * list walks the schema, which covers every variant of an enum two
         * messages or more share. An enum only one message uses never moves, so
         * that message is given a fixture per variant instead — named after the
         * variant, because `recording-signal-event-delete` says what it is and
         * `recording-signal-event-2` does not.
         */
        private List<Variant> variantsFor(Message message) {
            if (VARIANTS.containsKey(message.name())) {
                return VARIANTS.get(message.name());
            }
            for (Field field : message.fields()) {
                if (field.kind() != Kind.ENUM || messagesUsing(field.type().enumClass()) != 1) {
                    continue;
                }
                List<Variant> variants = new ArrayList<>();
                for (String value : field.type().enumNames()) {
                    variants.add(new Variant(kebabUpper(value), Map.of(field.name(), value)));
                }
                return List.copyOf(variants);
            }
            return List.of(new Variant("", Map.of()));
        }

        /** How many messages name this enum at least once. */
        private int messagesUsing(String enumClass) {
            int count = 0;
            for (Message message : messages) {
                boolean named = message.fields().stream()
                    .anyMatch(f -> f.kind() == Kind.ENUM && f.type().enumClass().equals(enumClass));
                count += named ? 1 : 0;
            }
            return count;
        }
    }

    private static Message parseMessage(Element element, Map<String, TypeSpec> types) {
        String name = element.getAttribute("name");
        List<Field> fields = new ArrayList<>();

        // Document order, and it is the order the encoder writes them in: the
        // fixed block first, then the variable-length fields behind it. The
        // mark schema lists a field carrying an explicit `offset=` after the
        // ones before it and before its data fields, which is exactly right.
        for (String tag : List.of("field", "data")) {
            for (Element field : children(element, tag)) {
                String typeName = field.getAttribute("type");
                TypeSpec type = types.get(typeName);
                if (type == null) {
                    throw new IllegalStateException(name + "." + field.getAttribute("name")
                        + " names the type " + typeName + ", which this program does not know");
                }
                // A field is optional if the schema says so on the field or on
                // the type alias it names — the mark schema declares its three
                // optional fields the second way, and only the second way.
                fields.add(new Field(
                    field.getAttribute("name"),
                    type,
                    intOrZero(field.getAttribute("sinceVersion")),
                    "optional".equals(field.getAttribute("presence")) || type.optional()));
            }
        }

        return new Message(
            Integer.parseInt(element.getAttribute("id")), name, kebab(name), List.copyOf(fields));
    }

    private static Map<String, TypeSpec> parseTypes(Element root, String packageName) {
        Map<String, TypeSpec> types = new LinkedHashMap<>();
        for (Element element : children(child(root, "types"), "*")) {
            switch (element.getLocalName()) {
                case "composite" -> {
                    switch (element.getAttribute("name")) {
                        case "varAsciiEncoding" -> types.put("varAsciiEncoding", TypeSpec.variable(Kind.VAR_ASCII));
                        case "varDataEncoding" -> types.put("varDataEncoding", TypeSpec.variable(Kind.VAR_DATA));
                        // `messageHeader` and `groupSizeEncoding` are never a
                        // field's type, and neither schema has a group.
                        default -> { }
                    }
                }
                case "enum" -> types.put(element.getAttribute("name"),
                    TypeSpec.enumeration(packageName + "." + element.getAttribute("name"), element));
                case "set" -> throw new IllegalStateException(
                    element.getAttribute("name") + " is a set, which this program cannot place");
                case "type" -> types.put(element.getAttribute("name"), TypeSpec.integer(element));
                default -> { }
            }
        }

        for (String primitive : List.of("int8", "int16", "int32", "int64")) {
            types.put(primitive, TypeSpec.primitive(primitive));
        }
        return types;
    }

    // ----------------------------------------------------------------- types

    enum Kind { INTEGER, ENUM, VAR_ASCII, VAR_DATA }

    /**
     * A resolved field type.
     *
     * `minValue`, `maxValue` and `nullValue` come from the schema where it
     * states them — the optional `version_t` declares all three — so a value
     * this program invents is inside the range the schema allows and not merely
     * inside the width of the integer.
     */
    record TypeSpec(Kind kind, String primitive, Long min, Long max, Long nullValue,
                    String enumClass, List<String> enumNames, boolean optional) {
        static TypeSpec primitive(String primitive) {
            return new TypeSpec(Kind.INTEGER, primitive, null, null, defaultNull(primitive), null, null, false);
        }

        static TypeSpec integer(Element type) {
            String primitive = type.getAttribute("primitiveType");
            Long declared = numberOrNull(type.getAttribute("nullValue"));
            return new TypeSpec(Kind.INTEGER, primitive,
                numberOrNull(type.getAttribute("minValue")),
                numberOrNull(type.getAttribute("maxValue")),
                declared != null ? declared : defaultNull(primitive), null, null,
                "optional".equals(type.getAttribute("presence")));
        }

        static TypeSpec enumeration(String className, Element type) {
            List<String> names = new ArrayList<>();
            for (Element value : children(type, "validValue")) {
                names.add(value.getAttribute("name"));
            }
            return new TypeSpec(Kind.ENUM, "int32", null, null, null, className, List.copyOf(names), false);
        }

        static TypeSpec variable(Kind kind) {
            return new TypeSpec(kind, null, null, null, null, null, null, false);
        }

        /**
         * The `sbe-tool` name for this type, as `golden.tsv` writes it.
         *
         * A field the schema marks `presence="optional"` carries its null value
         * with it, because that value is not recoverable from the reading: an
         * optional field's null and a field that is really zero read the same
         * in the table, and only the schema knows which it was. The decoders
         * `sbe-tool` generates answer `None` for exactly that value, so the
         * Rust side cannot check one without knowing the other.
         */
        String typeName(Field field) {
            String name = switch (kind) {
                case INTEGER -> switch (primitive) {
                    case "int8" -> "i8";
                    case "int16" -> "i16";
                    case "int32" -> "i32";
                    case "int64" -> "i64";
                    default -> throw new IllegalStateException("no golden name for " + primitive);
                };
                case ENUM -> "enum:" + enumClass.substring(enumClass.lastIndexOf('.') + 1);
                case VAR_ASCII -> "ascii";
                case VAR_DATA -> "data";
            };
            if (!field.optional()) {
                return name;
            }
            if (kind != Kind.INTEGER) {
                throw new IllegalStateException(
                    field.name() + " is optional and " + kind + ", which this program cannot name");
            }
            return name + ":null=" + nullValue;
        }
    }

    /** SBE's null for a primitive, where the schema does not state one. */
    private static long defaultNull(String primitive) {
        return switch (primitive) {
            case "int8" -> Byte.MIN_VALUE;
            case "int16" -> Short.MIN_VALUE;
            case "int32" -> Integer.MIN_VALUE;
            case "int64" -> Long.MIN_VALUE;
            default -> throw new IllegalStateException("no null value for " + primitive);
        };
    }

    record Field(String name, TypeSpec type, int sinceVersion, boolean optional) {
        Kind kind() {
            return type.kind();
        }
    }

    // -------------------------------------------------------------- fixtures

    /** One message in one shape: the bytes the reference made, and how it reads them. */
    record Fixture(Schema schema, Message message, int ordinal, Variant variant) {
        String name() {
            return variant.fixtureName(message.kebab());
        }

        String file() {
            return String.format("%d-%03d-%s.bin", schema.id(), message.id(), name());
        }

        /**
         * The acting versions this fixture is read at.
         *
         * The first is the schema's own — the bytes carry it in their header,
         * and it is the only version anything actually encodes. Any others are
         * the versions just below a field's `sinceVersion`, where the decoder
         * has to answer with a null instead of the bytes that are physically
         * there.
         *
         * They are readings, not fixtures, because `sbe-tool`'s Java encoder has
         * no version guard at all — it writes every field whatever the header
         * says — so no old version can be *encoded*, and the reference never
         * does. What can be asked of the reference is what it *reads*.
         */
        List<Integer> readings() {
            int lowest = message.fields().stream()
                .mapToInt(Field::sinceVersion).filter(v -> v > 0).min().orElse(0);
            return lowest == 0 ? List.of(schema.version()) : List.of(schema.version(), lowest - 1);
        }

        List<Reading> run(Path out) throws Exception {
            ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(4096);

            Class<?> encoderClass = Class.forName(schema.packageName() + "." + message.name() + "Encoder");
            Class<?> headerEncoderClass = Class.forName(schema.packageName() + ".MessageHeaderEncoder");
            Object encoder = encoderClass.getDeclaredConstructor().newInstance();
            Object headerEncoder = headerEncoderClass.getDeclaredConstructor().newInstance();

            encoderClass.getMethod("wrapAndApplyHeader", MutableDirectBuffer.class, int.class, headerEncoderClass)
                .invoke(encoder, buffer, 0, headerEncoder);

            for (Field field : message.fields()) {
                setField(encoder, field, valueOf(field));
            }

            int length = MESSAGE_HEADER_LENGTH + (int) encoderClass.getMethod("encodedLength").invoke(encoder);
            byte[] bytes = new byte[length];
            buffer.getBytes(0, bytes, 0, length);
            Files.write(out.resolve(file()), bytes);

            List<Reading> readings = new ArrayList<>();
            for (int version : readings()) {
                readings.addAll(read(bytes, version));
            }
            return readings;
        }

        /** The rule, field by field. The header comment says what it is and why. */
        private Object valueOf(Field field) {
            String override = variant.overrides().get(field.name());
            return switch (field.kind()) {
                case INTEGER -> {
                    if (override != null) {
                        yield Long.parseLong(override);
                    }
                    long lo = field.type().min() != null && field.type().min() > 1 ? field.type().min() : 1;
                    long hi = field.type().max() != null ? field.type().max() : 100;
                    yield lo + Math.floorMod(fnv1a(field.name()), (int) Math.min(100, hi - lo + 1));
                }
                case ENUM -> {
                    List<String> names = field.type().enumNames();
                    yield override != null ? override : names.get(ordinal % names.size());
                }
                case VAR_ASCII -> override != null ? override : field.name();
                case VAR_DATA -> (override != null ? override : field.name())
                    .getBytes(StandardCharsets.US_ASCII);
            };
        }

        /**
         * The reference's reading of these bytes at one acting version.
         *
         * Every value is what `io.aeron.archive.codecs.*Decoder` answers; this
         * program computes nothing about what the answer should be. That is the
         * difference between a golden and a hope.
         *
         * A version below the schema's own is read from a copy whose header
         * carries that version, because that byte is what tells the decoder how
         * much of the block it may believe. The committed `.bin` keeps the
         * version the encoder wrote.
         */
        private List<Reading> read(byte[] bytes, int version) throws Exception {
            byte[] at = bytes;
            if (version != schema.version()) {
                at = bytes.clone();
                at[HEADER_VERSION_OFFSET] = (byte) version;
                at[HEADER_VERSION_OFFSET + 1] = (byte) (version >>> 8);
            }

            ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(at.length);
            buffer.putBytes(0, at, 0, at.length);

            Class<?> headerDecoderClass = Class.forName(schema.packageName() + ".MessageHeaderDecoder");
            Object header = headerDecoderClass.getDeclaredConstructor().newInstance();
            headerDecoderClass.getMethod("wrap", DirectBuffer.class, int.class).invoke(header, buffer, 0);
            int blockLength = (int) headerDecoderClass.getMethod("blockLength").invoke(header);

            List<Reading> readings = new ArrayList<>();
            for (String field : List.of("blockLength", "templateId", "schemaId", "version")) {
                long value = ((Number) headerDecoderClass.getMethod(field).invoke(header)).longValue();
                readings.add(new Reading(name(), version, "header", field, "u16", Long.toString(value)));
            }

            Class<?> decoderClass = Class.forName(schema.packageName() + "." + message.name() + "Decoder");
            Object decoder = decoderClass.getDeclaredConstructor().newInstance();
            decoderClass.getMethod("wrap", DirectBuffer.class, int.class, int.class, int.class)
                .invoke(decoder, buffer, MESSAGE_HEADER_LENGTH, blockLength, version);

            for (Field field : message.fields()) {
                readings.add(new Reading(name(), version, "field", field.name(),
                    field.type().typeName(field), readField(decoder, field)));
            }
            return readings;
        }

        private static String readField(Object decoder, Field field) throws Exception {
            Class<?> type = decoder.getClass();
            switch (field.kind()) {
                case INTEGER -> {
                    Object value = type.getMethod(field.name()).invoke(decoder);
                    if (!(value instanceof Byte || value instanceof Short
                        || value instanceof Integer || value instanceof Long)) {
                        throw new IllegalStateException(field.name() + " decoded to " + value.getClass());
                    }
                    return value.toString();
                }
                case ENUM -> {
                    Object constant = type.getMethod(field.name()).invoke(decoder);
                    int raw = (int) constant.getClass().getMethod("value").invoke(constant);
                    return ((Enum<?>) constant).name() + ":" + raw;
                }
                default -> {
                    int length = (int) type.getMethod(field.name() + "Length").invoke(decoder);
                    byte[] data = new byte[length];
                    type.getMethod("get" + capitalize(field.name()), byte[].class, int.class, int.class)
                        .invoke(decoder, data, 0, length);
                    return field.kind() == Kind.VAR_ASCII
                        ? new String(data, StandardCharsets.US_ASCII)
                        : hex(data);
                }
            }
        }
    }

    /** One line of `golden.tsv` that is not a fixture line. */
    record Reading(String fixture, int version, String tag, String field, String type, String value) { }

    // ------------------------------------------------------------ reflection

    /**
     * The setter, found by the name the schema gives the field.
     *
     * `sbe-tool` names the generated Java methods after the schema, so nothing
     * here is a guess: `fieldName` for a field, `putFieldName` for a
     * variable-length one. A field that has no setter is a `NoSuchMethod`
     * away from stopping the run, which is the point — a field silently left at
     * zero would make a fixture that passes for the wrong reason.
     */
    private static void setField(Object encoder, Field field, Object value) throws Exception {
        Class<?> type = encoder.getClass();
        switch (field.kind()) {
            case INTEGER -> {
                String primitive = field.type().primitive();
                type.getMethod(field.name(), javaType(primitive)).invoke(encoder, narrow((Long) value, primitive));
            }
            case ENUM -> {
                Class<?> enumClass = Class.forName(field.type().enumClass());
                type.getMethod(field.name(), enumClass).invoke(encoder, enumValue(enumClass, (String) value));
            }
            case VAR_ASCII -> type.getMethod(field.name(), CharSequence.class).invoke(encoder, value);
            case VAR_DATA -> {
                byte[] data = (byte[]) value;
                type.getMethod("put" + capitalize(field.name()), byte[].class, int.class, int.class)
                    .invoke(encoder, data, 0, data.length);
            }
        }
    }

    private static Class<?> javaType(String primitive) {
        return switch (primitive) {
            case "int8" -> byte.class;
            case "int16" -> short.class;
            case "int32" -> int.class;
            case "int64" -> long.class;
            default -> throw new IllegalStateException("no Java setter type for " + primitive);
        };
    }

    private static Object narrow(long value, String primitive) {
        return switch (primitive) {
            case "int8" -> (byte) value;
            case "int16" -> (short) value;
            case "int32" -> (int) value;
            case "int64" -> value;
            default -> throw new IllegalStateException("no Java setter type for " + primitive);
        };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object enumValue(Class<?> enumClass, String name) {
        return Enum.valueOf((Class<Enum>) enumClass, name);
    }

    // ---------------------------------------------------------------- output

    /**
     * A `.bin` no message produces is a fixture left over from a schema that
     * has moved on. Refused rather than overwritten around: a stale golden that
     * still passes is the one kind of green nobody can trust.
     */
    private static void refuseStrays(Path out, List<Fixture> fixtures) throws Exception {
        TreeSet<String> expected = new TreeSet<>();
        for (Fixture fixture : fixtures) {
            expected.add(fixture.file());
        }
        try (Stream<Path> entries = Files.list(out)) {
            for (Path path : entries.toList()) {
                String fileName = path.getFileName().toString();
                if (fileName.endsWith(".bin") && !expected.contains(fileName)) {
                    throw new IllegalStateException(fileName + " is not a fixture any more; remove it");
                }
            }
        }
    }

    private static void write(Path out, List<Fixture> fixtures, List<Reading> readings) throws Exception {
        StringBuilder tsv = new StringBuilder();
        tsv.append("# SBE golden fixtures. Generated by Generate.java — do not edit.\n");
        tsv.append("#\n");
        tsv.append("# fixture <name> <file> <schemaId> <templateId> <version> <bytes>\n");
        tsv.append("# header  <name> <version> <field> <type> <value>\n");
        tsv.append("# field   <name> <version> <field> <type> <value>\n");
        tsv.append("#\n");
        tsv.append("# Every `header` and `field` line is what the reference decoder answers for the\n");
        tsv.append("# bytes in that file at that acting version — not what the generator put in.\n");
        tsv.append("# A version below the message's own is a reading of the same bytes with the\n");
        tsv.append("# header's version lowered; README.md says why that is the only way to get one.\n");
        tsv.append("#\n");
        tsv.append("# name\tfile\tschemaId\ttemplateId\tversion\tbytes\n");

        for (Fixture fixture : fixtures) {
            tsv.append("fixture\t").append(fixture.name())
                .append('\t').append(fixture.file())
                .append('\t').append(fixture.schema().id())
                .append('\t').append(fixture.message().id())
                .append('\t').append(fixture.readings().get(0))
                .append('\t').append(Files.size(out.resolve(fixture.file())))
                .append('\n');
        }

        tsv.append("#\n");
        for (Reading reading : readings) {
            tsv.append(reading.tag())
                .append('\t').append(reading.fixture())
                .append('\t').append(reading.version())
                .append('\t').append(reading.field())
                .append('\t').append(reading.type())
                .append('\t').append(reading.value())
                .append('\n');
        }

        Files.writeString(out.resolve("golden.tsv"), tsv.toString(), StandardCharsets.UTF_8);
    }

    // ----------------------------------------------------------------- utils

    private static List<Element> children(Element parent, String localName) {
        List<Element> found = new ArrayList<>();
        if (parent == null) {
            return found;
        }
        NodeList nodes = parent.getChildNodes();
        for (int i = 0; i < nodes.getLength(); i++) {
            Node node = nodes.item(i);
            if (node instanceof Element element
                && ("*".equals(localName) || localName.equals(element.getLocalName()))) {
                found.add(element);
            }
        }
        return found;
    }

    private static Element child(Element parent, String localName) {
        List<Element> found = children(parent, localName);
        return found.isEmpty() ? null : found.get(0);
    }

    /** The schema omits `sinceVersion` for a field that has been there since the start. */
    private static int intOrZero(String value) {
        return value == null || value.isEmpty() ? 0 : Integer.parseInt(value);
    }

    private static Long numberOrNull(String value) {
        return value == null || value.isEmpty() ? null : Long.parseLong(value);
    }

    /** 32-bit FNV-1a. Any stable hash would do; this one is four lines and needs no import. */
    private static int fnv1a(String text) {
        int hash = 0x811c9dc5;
        for (byte b : text.getBytes(StandardCharsets.US_ASCII)) {
            hash = (hash ^ (b & 0xff)) * 0x01000193;
        }
        return hash;
    }

    private static String kebab(String camel) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < camel.length(); i++) {
            char c = camel.charAt(i);
            if (i > 0 && Character.isUpperCase(c) && !Character.isUpperCase(camel.charAt(i - 1))) {
                out.append('-');
            } else if (i > 0 && Character.isDigit(c) && !Character.isDigit(camel.charAt(i - 1))) {
                out.append('-');
            }
            out.append(Character.toLowerCase(c));
        }
        return out.toString();
    }

    /** The schema spells an enum's valid values in upper snake case; a fixture name is not. */
    private static String kebabUpper(String value) {
        return value.toLowerCase().replace('_', '-');
    }

    private static String capitalize(String name) {
        return Character.toUpperCase(name.charAt(0)) + name.substring(1);
    }

    private static String hex(byte[] data) {
        StringBuilder out = new StringBuilder(data.length * 2);
        for (byte b : data) {
            out.append(String.format("%02x", b));
        }
        return out.toString();
    }

    private Generate() {
    }
}
