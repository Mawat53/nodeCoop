// src/controllers/memberController.js
const memberModel = require('../models/memberModel');

async function listMembers(req, res) {
    try {
        const members = await memberModel.getAllMembers();
        res.status(200).json(members);
    } catch (err) {
        // แก้ไขตรงนี้เพื่อดู error ที่แท้จริง
        console.error('Error in memberController:', err.message);
        res.status(500).json({ error: 'Failed to retrieve members' });
    }
}

async function createMember(req, res) {
    try {
        const member = req.body;
        const rowsAffected = await memberModel.createMember(member);
        res.status(201).json({ message: `${rowsAffected} member created successfully.` });
    } catch (err) {
        console.error('Error creating member:', err.message);
        res.status(500).json({ error: 'Failed to create member', details: err.message });
    }
}

async function updateMember(req, res) {
    try {
        const { memberId } = req.params;
        const member = req.body;
        const rowsAffected = await memberModel.updateMember(memberId, member);

        if (rowsAffected === 0) {
            return res.status(404).json({ message: 'Member not found.' });
        }
        res.status(200).json({ message: `${rowsAffected} member updated successfully.` });
    } catch (err) {
        console.error('Error updating member:', err.message);
        res.status(500).json({ error: 'Failed to update member', details: err.message });
    }
}


module.exports = {
    listMembers,
    createMember,
    updateMember,

};