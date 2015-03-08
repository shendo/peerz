# Peerz - P2P python library using ZeroMQ sockets and gevent
# Copyright (C) 2014 Steve Henderson
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import time

from M2Crypto import X509, EVP, RSA, ASN1

# Lovingly lifted from M2Crypto unit test cases...
def callback(*args):
    """
    Empty callback for key generation.
    """
    pass

def mkreq(bits):
    """
    Generates a new RSA keypair and a signing request.
    @param bits: Key size in bits
    @return: Tuple of signing request, private key
    """
    pk = EVP.PKey()
    x = X509.Request()
    rsa = RSA.gen_key(bits, 65537, callback)
    pk.assign_rsa(rsa)
    x.set_pubkey(pk)
    name = x.get_subject()
    name.C = "US"
    name.CN = "Anonymous"
    x.sign(pk,'sha1')
    return x, pk

def generate_self_signed_cert():
    """
    Generates a new X.509 self signed certificate.
    Details of certificate are currently defaulted.
    @return X509 certificate object
    """
    req, pk = mkreq(4096)
    pkey = req.get_pubkey()
    sub = req.get_subject()
    cert = X509.X509()
    cert.set_serial_number(1)
    cert.set_version(1)
    cert.set_subject(sub)
    t = long(time.time()) + time.timezone
    now = ASN1.ASN1_UTCTIME()
    now.set_time(t)
    nowPlusYear = ASN1.ASN1_UTCTIME()
    nowPlusYear.set_time(t + 60 * 60 * 24 * 365)
    cert.set_not_before(now)
    cert.set_not_after(nowPlusYear)
    issuer = X509.X509_Name()
    issuer.CN = 'Peerz P2P Library'
    cert.set_issuer(issuer)
    cert.set_pubkey(pkey)
    cert.sign(pk, 'sha1')
    return cert
