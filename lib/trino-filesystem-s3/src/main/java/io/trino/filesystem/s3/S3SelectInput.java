package io.trino.filesystem.s3;

import io.trino.filesystem.Location;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.EndEvent;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.RecordsEvent;
import software.amazon.awssdk.services.s3.model.ScanRange;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponseHandler;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayDeque;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.Objects.requireNonNull;

public class S3SelectInput
        extends AbstractS3Input<SelectObjectContentRequest>
{
    private final S3AsyncClient asyncClient;
    private final SelectObjectContentRequest request;

    public S3SelectInput(Location location, S3Client client, S3AsyncClient asyncClient, SelectObjectContentRequest request)
    {
        super(location, client);
        this.asyncClient = requireNonNull(asyncClient, "asyncClient is null");
        this.request = requireNonNull(request, "request is null");
    }

    @Override
    SelectObjectContentRequest getRequest(Optional<Long> position, long length)
    {
        ScanRange.Builder scanRange = ScanRange.builder()
                .end(position.orElse(0L) + length - 1);
        position.ifPresent(scanRange::start);
        return request.toBuilder()
                .scanRange(scanRange.build())
                .build();
    }

    @Override
    protected InputStream streamForRequest(SelectObjectContentRequest request, S3Client client, Location location)
            throws IOException
    {
        try {
            EventStreamEnumeration eventStreamEnumeration = new EventStreamEnumeration();
            InputStream recordInputStream = new SequenceInputStream(eventStreamEnumeration);

            SelectObjectContentResponseHandler.Visitor visitor = new SelectObjectContentResponseHandler.Visitor()
            {
                @Override
                public void visitRecords(RecordsEvent event)
                {
                    eventStreamEnumeration.addEvent(new S3Event(event, false));
                }

                @Override
                public void visitEnd(EndEvent event)
                {
                    eventStreamEnumeration.addEvent(new S3Event(null, true));
                }
            };

            asyncClient.selectObjectContent(request, SelectObjectContentResponseHandler.builder().subscriber(visitor).build());
            return recordInputStream;
        }
        catch (NoSuchKeyException e) {
            throw new FileNotFoundException(location.toString());
        }
        catch (SdkException e) {
            throw new IOException("Failed to open S3 file: " + location, e);
        }
    }

    /**
     * Below classes are required for compatibility between AWS Java SDK 1.x and 2.x
     * They return an InputStream to all the incoming record events
     */
    static class S3Event
    {
        RecordsEvent event;
        boolean isEndEvent;

        public S3Event(RecordsEvent event, boolean isEndEvent)
        {
            this.event = event;
            this.isEndEvent = isEndEvent;
        }
    }

    private static class EventStreamEnumeration
            extends LazyLoadedIterator<InputStream> implements Enumeration<InputStream>
    {
        private boolean initialized;
        private final BlockingQueue<S3Event> inputStreams;

        EventStreamEnumeration()
        {
            this.inputStreams = new LinkedBlockingQueue<>();
        }

        @Override
        protected Optional<? extends InputStream> getNext()
                throws InterruptedException
        {
            if (!initialized) {
                initialized = true;
                return Optional.of(new ByteArrayInputStream(new byte[0]));
            }

            S3Event s3Event = inputStreams.take();
            if (s3Event.isEndEvent) {
                return Optional.empty();
            }
            return Optional.of(s3Event.event.payload().asInputStream());
        }

        public void addEvent(S3Event event)
        {
            this.inputStreams.add(event);
        }

        @Override
        public boolean hasMoreElements()
        {
            return super.hasNext();
        }

        @Override
        public InputStream nextElement()
        {
            return super.next();
        }
    }

    private abstract static class LazyLoadedIterator<T>
            implements Iterator<T>
    {
        private final Queue<T> next = new ArrayDeque<T>();
        private boolean isDone;

        @Override
        public boolean hasNext()
        {
            advanceIfNeeded();
            return !isDone;
        }

        @Override
        public T next()
        {
            advanceIfNeeded();

            if (isDone) {
                throw new NoSuchElementException();
            }

            return next.poll();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        private void advanceIfNeeded()
        {
            if (!isDone && next.isEmpty()) {
                try {
                    Optional<? extends T> nextElement = getNext();
                    nextElement.ifPresent(this.next::add);
                    this.isDone = this.next.isEmpty();
                }
                catch (InterruptedException e) {
                    throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Interrupted"); // TODO: Better error message
                }
            }
        }

        /**
         * Load any newly-available events. This can return any number of events, in the order they should be encountered by the
         * user of the iterator. This should return an empty collection if there are no remaining events in the stream.
         */
        protected abstract Optional<? extends T> getNext()
                throws InterruptedException;
    }
}
