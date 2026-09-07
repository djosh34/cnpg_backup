# No build-time package manager or floating builder image. hack/build.py prepares
# checksum-locked inputs; only these audited roots enter the runtime images.
FROM scratch AS manager
COPY build/out/manager/ /
USER 26:26
ENTRYPOINT ["/usr/local/bin/cnpg-backup"]
CMD ["manager"]

FROM scratch AS pg18
COPY build/out/pg18/ /
ENV PATH=/usr/local/bin:/usr/lib/postgresql/18/bin
USER 26:26
ENTRYPOINT ["/usr/local/bin/cnpg-backup"]
CMD ["instance"]
